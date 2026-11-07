// Package flow implements the optional orchestration DSL. A flow is compiled
// into an immutable, validated plan and executed by the durable step engine;
// agent and tool nodes run as child runs so leases, approvals and recovery
// keep their existing semantics.
package flow

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unicode"
)

// Expr is a compiled expression. Expressions are pure: they only read the
// evaluation context and never call tools or the model.
type Expr interface {
	Eval(ctx map[string]any) (any, error)
	// Refs reports static path prefixes such as ["nodes","triage","output"].
	Refs() [][]string
}

type literalExpr struct{ v any }

func (e literalExpr) Eval(map[string]any) (any, error) { return e.v, nil }
func (e literalExpr) Refs() [][]string                 { return nil }

type pathStep struct {
	field string
	index Expr // when field is empty
}

type pathExpr struct {
	root  string
	steps []pathStep
}

func (e pathExpr) Eval(ctx map[string]any) (any, error) {
	cur, ok := ctx[e.root]
	if !ok {
		return nil, nil
	}
	for _, st := range e.steps {
		if st.field != "" {
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, nil
			}
			cur = m[st.field]
			continue
		}
		idx, err := st.index.Eval(ctx)
		if err != nil {
			return nil, err
		}
		switch key := idx.(type) {
		case string:
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, nil
			}
			cur = m[key]
		case float64:
			arr, ok := cur.([]any)
			if !ok {
				return nil, nil
			}
			i := int(key)
			if i < 0 || i >= len(arr) {
				return nil, nil
			}
			cur = arr[i]
		default:
			return nil, fmt.Errorf("index must be a string or number, got %T", idx)
		}
	}
	return cur, nil
}

func (e pathExpr) Refs() [][]string {
	prefix := []string{e.root}
	var out [][]string
	for _, st := range e.steps {
		if st.field == "" {
			break
		}
		prefix = append(prefix, st.field)
	}
	out = append(out, prefix)
	return out
}

type unaryExpr struct {
	op string
	x  Expr
}

func (e unaryExpr) Eval(ctx map[string]any) (any, error) {
	v, err := e.x.Eval(ctx)
	if err != nil {
		return nil, err
	}
	switch e.op {
	case "!", "not":
		return !truthy(v), nil
	case "-":
		n, ok := number(v)
		if !ok {
			return nil, fmt.Errorf("unary - needs a number, got %T", v)
		}
		return -n, nil
	case "+":
		n, ok := number(v)
		if !ok {
			return nil, fmt.Errorf("unary + needs a number, got %T", v)
		}
		return n, nil
	}
	return nil, fmt.Errorf("unknown unary operator %q", e.op)
}

func (e unaryExpr) Refs() [][]string { return e.x.Refs() }

type binaryExpr struct {
	op   string
	l, r Expr
}

func (e binaryExpr) Refs() [][]string { return append(e.l.Refs(), e.r.Refs()...) }

func (e binaryExpr) Eval(ctx map[string]any) (any, error) {
	if e.op == "&&" || e.op == "and" {
		l, err := e.l.Eval(ctx)
		if err != nil {
			return nil, err
		}
		if !truthy(l) {
			return false, nil
		}
		r, err := e.r.Eval(ctx)
		if err != nil {
			return nil, err
		}
		return truthy(r), nil
	}
	if e.op == "||" || e.op == "or" {
		l, err := e.l.Eval(ctx)
		if err != nil {
			return nil, err
		}
		if truthy(l) {
			return true, nil
		}
		r, err := e.r.Eval(ctx)
		if err != nil {
			return nil, err
		}
		return truthy(r), nil
	}
	l, err := e.l.Eval(ctx)
	if err != nil {
		return nil, err
	}
	r, err := e.r.Eval(ctx)
	if err != nil {
		return nil, err
	}
	switch e.op {
	case "==":
		return equal(l, r), nil
	case "!=":
		return !equal(l, r), nil
	case "<", "<=", ">", ">=":
		return compare(e.op, l, r)
	case "in":
		return containsValue(r, l), nil
	case "+":
		if _, ok := l.(string); ok {
			return stringify(l) + stringify(r), nil
		}
		if _, ok := r.(string); ok {
			return stringify(l) + stringify(r), nil
		}
		ln, lok := number(l)
		rn, rok := number(r)
		if !lok || !rok {
			return nil, fmt.Errorf("+ needs numbers or strings, got %T and %T", l, r)
		}
		return ln + rn, nil
	case "-", "*", "/", "%":
		ln, lok := number(l)
		rn, rok := number(r)
		if !lok || !rok {
			return nil, fmt.Errorf("%s needs numbers, got %T and %T", e.op, l, r)
		}
		switch e.op {
		case "-":
			return ln - rn, nil
		case "*":
			return ln * rn, nil
		case "/":
			if rn == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return ln / rn, nil
		default:
			if rn == 0 {
				return nil, fmt.Errorf("modulo by zero")
			}
			return math.Mod(ln, rn), nil
		}
	}
	return nil, fmt.Errorf("unknown operator %q", e.op)
}

type callExpr struct {
	name string
	args []Expr
}

func (e callExpr) Refs() [][]string {
	var out [][]string
	for _, a := range e.args {
		out = append(out, a.Refs()...)
	}
	return out
}

func (e callExpr) Eval(ctx map[string]any) (any, error) {
	args := make([]any, 0, len(e.args))
	for _, a := range e.args {
		v, err := a.Eval(ctx)
		if err != nil {
			return nil, err
		}
		args = append(args, v)
	}
	switch e.name {
	case "len":
		if len(args) != 1 {
			return nil, fmt.Errorf("len takes one argument")
		}
		switch v := args[0].(type) {
		case string:
			return float64(len([]rune(v))), nil
		case []any:
			return float64(len(v)), nil
		case map[string]any:
			return float64(len(v)), nil
		default:
			return nil, fmt.Errorf("len does not accept %T", args[0])
		}
	case "lower", "upper":
		if len(args) != 1 {
			return nil, fmt.Errorf("%s takes one argument", e.name)
		}
		s, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("%s needs a string", e.name)
		}
		if e.name == "lower" {
			return strings.ToLower(s), nil
		}
		return strings.ToUpper(s), nil
	case "contains":
		if len(args) != 2 {
			return nil, fmt.Errorf("contains takes two arguments")
		}
		return containsValue(args[0], args[1]), nil
	case "coalesce":
		for _, a := range args {
			if a != nil && a != "" {
				return a, nil
			}
		}
		return nil, nil
	case "string":
		if len(args) != 1 {
			return nil, fmt.Errorf("string takes one argument")
		}
		return stringify(args[0]), nil
	case "number":
		if len(args) != 1 {
			return nil, fmt.Errorf("number takes one argument")
		}
		n, ok := number(args[0])
		if !ok {
			return nil, fmt.Errorf("cannot convert %T to number", args[0])
		}
		return n, nil
	case "bool":
		if len(args) != 1 {
			return nil, fmt.Errorf("bool takes one argument")
		}
		return truthy(args[0]), nil
	}
	return nil, fmt.Errorf("unknown function %q", e.name)
}

// CompileExpr parses src into a pure expression.
func CompileExpr(src string) (Expr, error) {
	toks, err := lexAll(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	e, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tokEOF {
		return nil, fmt.Errorf("unexpected token %q at %d", p.peek().text, p.peek().pos)
	}
	return e, nil
}

// ValidateExpr compiles src and reports every static reference to fn.
func ValidateExpr(src string, fn func(path []string) error) error {
	e, err := CompileExpr(src)
	if err != nil {
		return err
	}
	if fn == nil {
		return nil
	}
	for _, p := range e.Refs() {
		if err := fn(p); err != nil {
			return err
		}
	}
	return nil
}

// ---------- interpolation ----------

// Interpolate evaluates ${...} expressions inside a string. A string that is
// exactly one expression keeps the expression's type.
func Interpolate(s string, ctx map[string]any) (any, error) {
	parts, exprs, err := splitInterpolation(s)
	if err != nil {
		return nil, err
	}
	if len(exprs) == 0 {
		return s, nil
	}
	if len(exprs) == 1 && parts[0] == "" && parts[1] == "" {
		return exprs[0].Eval(ctx)
	}
	var b strings.Builder
	for i, part := range parts {
		b.WriteString(part)
		if i < len(exprs) {
			v, err := exprs[i].Eval(ctx)
			if err != nil {
				return nil, err
			}
			b.WriteString(stringify(v))
		}
	}
	return b.String(), nil
}

// InterpolateValue walks JSON-like values and interpolates every string.
func InterpolateValue(v any, ctx map[string]any) (any, error) {
	switch x := v.(type) {
	case string:
		return Interpolate(x, ctx)
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			iv, err := InterpolateValue(item, ctx)
			if err != nil {
				return nil, err
			}
			out[i] = iv
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, item := range x {
			iv, err := InterpolateValue(item, ctx)
			if err != nil {
				return nil, err
			}
			out[k] = iv
		}
		return out, nil
	default:
		return v, nil
	}
}

// ValidateInterpolation compiles every ${...} expression in s.
func ValidateInterpolation(s string, fn func(path []string) error) error {
	_, exprs, err := splitInterpolation(s)
	if err != nil {
		return err
	}
	for _, e := range exprs {
		if fn != nil {
			for _, p := range e.Refs() {
				if err := fn(p); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// ValidateInterpolations walks v and validates every embedded expression.
func ValidateInterpolations(v any, fn func(path []string) error) error {
	switch x := v.(type) {
	case string:
		return ValidateInterpolation(x, fn)
	case []any:
		for _, item := range x {
			if err := ValidateInterpolations(item, fn); err != nil {
				return err
			}
		}
	case map[string]any:
		for _, item := range x {
			if err := ValidateInterpolations(item, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

func splitInterpolation(s string) ([]string, []Expr, error) {
	var parts []string
	var exprs []Expr
	var lit strings.Builder
	for i := 0; i < len(s); {
		if s[i] == '\\' && i+2 < len(s) && s[i+1] == '$' && s[i+2] == '{' {
			lit.WriteString("${")
			i += 3
			continue
		}
		if s[i] != '$' || i+1 >= len(s) || s[i+1] != '{' {
			lit.WriteByte(s[i])
			i++
			continue
		}
		end, err := closingBrace(s, i+2)
		if err != nil {
			return nil, nil, err
		}
		src := strings.TrimSpace(s[i+2 : end])
		if src == "" {
			return nil, nil, fmt.Errorf("empty interpolation at %d", i)
		}
		e, err := CompileExpr(src)
		if err != nil {
			return nil, nil, fmt.Errorf("interpolation %q: %w", src, err)
		}
		parts = append(parts, lit.String())
		lit.Reset()
		exprs = append(exprs, e)
		i = end + 1
	}
	parts = append(parts, lit.String())
	return parts, exprs, nil
}

func closingBrace(s string, start int) (int, error) {
	var quote byte
	for i := start; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == '\\' {
				i++
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
		case '}':
			return i, nil
		}
	}
	return 0, fmt.Errorf("unterminated interpolation")
}

// ---------- lexer ----------

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokNumber
	tokString
	tokOp
	tokLParen
	tokRParen
	tokLBracket
	tokRBracket
	tokComma
	tokDot
)

type token struct {
	kind tokKind
	text string
	pos  int
}

func lexAll(src string) ([]token, error) {
	var out []token
	for i := 0; i < len(src); {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			out = append(out, token{tokLParen, "(", i})
			i++
		case c == ')':
			out = append(out, token{tokRParen, ")", i})
			i++
		case c == '[':
			out = append(out, token{tokLBracket, "[", i})
			i++
		case c == ']':
			out = append(out, token{tokRBracket, "]", i})
			i++
		case c == ',':
			out = append(out, token{tokComma, ",", i})
			i++
		case c == '.':
			out = append(out, token{tokDot, ".", i})
			i++
		case c == '\'' || c == '"':
			s, n, err := lexString(src, i)
			if err != nil {
				return nil, err
			}
			out = append(out, token{tokString, s, i})
			i += n
		case isDigit(c):
			start := i
			for i < len(src) && (isDigit(src[i]) || src[i] == '.') {
				i++
			}
			if i < len(src) && (src[i] == 'e' || src[i] == 'E') {
				i++
				if i < len(src) && (src[i] == '+' || src[i] == '-') {
					i++
				}
				for i < len(src) && isDigit(src[i]) {
					i++
				}
			}
			out = append(out, token{tokNumber, src[start:i], start})
		case isIdentStart(c):
			start := i
			for i < len(src) && isIdentPart(src[i]) {
				i++
			}
			out = append(out, token{tokIdent, src[start:i], start})
		default:
			two := ""
			if i+1 < len(src) {
				two = src[i : i+2]
			}
			switch two {
			case "==", "!=", "<=", ">=", "&&", "||":
				out = append(out, token{tokOp, two, i})
				i += 2
				continue
			}
			switch c {
			case '<', '>', '!', '+', '-', '*', '/', '%':
				out = append(out, token{tokOp, string(c), i})
				i++
			default:
				return nil, fmt.Errorf("unexpected character %q at %d", c, i)
			}
		}
	}
	out = append(out, token{tokEOF, "", len(src)})
	return out, nil
}

func lexString(src string, start int) (string, int, error) {
	quote := src[start]
	var b strings.Builder
	for i := start + 1; i < len(src); i++ {
		c := src[i]
		if c == '\\' {
			if i+1 >= len(src) {
				return "", 0, fmt.Errorf("unterminated string at %d", start)
			}
			i++
			switch src[i] {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			case '\\', '\'', '"':
				b.WriteByte(src[i])
			default:
				b.WriteByte('\\')
				b.WriteByte(src[i])
			}
			continue
		}
		if c == quote {
			return b.String(), i - start + 1, nil
		}
		b.WriteByte(c)
	}
	return "", 0, fmt.Errorf("unterminated string at %d", start)
}

func isDigit(c byte) bool      { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool { return c == '_' || unicode.IsLetter(rune(c)) }
func isIdentPart(c byte) bool  { return isIdentStart(c) || isDigit(c) }

// ---------- parser ----------

type parser struct {
	toks []token
	pos  int
}

func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) next() token {
	t := p.toks[p.pos]
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}
func (p *parser) matchOp(ops ...string) (string, bool) {
	t := p.peek()
	if t.kind != tokOp && t.kind != tokIdent {
		return "", false
	}
	for _, op := range ops {
		if t.text == op {
			p.next()
			return op, true
		}
	}
	return "", false
}
func (p *parser) expect(k tokKind, text string) (token, error) {
	t := p.peek()
	if t.kind != k {
		return t, fmt.Errorf("expected %s at %d, got %q", text, t.pos, t.text)
	}
	p.next()
	return t, nil
}

func (p *parser) parseOr() (Expr, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for {
		op, ok := p.matchOp("||", "or")
		if !ok {
			return l, nil
		}
		r, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		l = binaryExpr{op, l, r}
	}
}

func (p *parser) parseAnd() (Expr, error) {
	l, err := p.parseCompare()
	if err != nil {
		return nil, err
	}
	for {
		op, ok := p.matchOp("&&", "and")
		if !ok {
			return l, nil
		}
		r, err := p.parseCompare()
		if err != nil {
			return nil, err
		}
		l = binaryExpr{op, l, r}
	}
}

func (p *parser) parseCompare() (Expr, error) {
	l, err := p.parseAdd()
	if err != nil {
		return nil, err
	}
	op, ok := p.matchOp("==", "!=", "<", "<=", ">", ">=", "in")
	if !ok {
		return l, nil
	}
	r, err := p.parseAdd()
	if err != nil {
		return nil, err
	}
	return binaryExpr{op, l, r}, nil
}

func (p *parser) parseAdd() (Expr, error) {
	l, err := p.parseMul()
	if err != nil {
		return nil, err
	}
	for {
		op, ok := p.matchOp("+", "-")
		if !ok {
			return l, nil
		}
		r, err := p.parseMul()
		if err != nil {
			return nil, err
		}
		l = binaryExpr{op, l, r}
	}
}

func (p *parser) parseMul() (Expr, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		op, ok := p.matchOp("*", "/", "%")
		if !ok {
			return l, nil
		}
		r, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		l = binaryExpr{op, l, r}
	}
}

func (p *parser) parseUnary() (Expr, error) {
	if op, ok := p.matchOp("!", "not", "-", "+"); ok {
		x, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return unaryExpr{op, x}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (Expr, error) {
	t := p.peek()
	switch t.kind {
	case tokNumber:
		p.next()
		n, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number %q at %d", t.text, t.pos)
		}
		return literalExpr{n}, nil
	case tokString:
		p.next()
		return literalExpr{t.text}, nil
	case tokLParen:
		p.next()
		e, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokRParen, ")"); err != nil {
			return nil, err
		}
		return e, nil
	case tokLBracket:
		p.next()
		var items []Expr
		if p.peek().kind != tokRBracket {
			for {
				item, err := p.parseOr()
				if err != nil {
					return nil, err
				}
				items = append(items, item)
				if p.peek().kind != tokComma {
					break
				}
				p.next()
			}
		}
		if _, err := p.expect(tokRBracket, "]"); err != nil {
			return nil, err
		}
		return arrayExpr{items}, nil
	case tokIdent:
		p.next()
		switch t.text {
		case "true":
			return literalExpr{true}, nil
		case "false":
			return literalExpr{false}, nil
		case "null":
			return literalExpr{nil}, nil
		}
		if p.peek().kind == tokLParen {
			return p.parseCall(t.text)
		}
		return p.parsePath(t.text)
	}
	return nil, fmt.Errorf("unexpected token %q at %d", t.text, t.pos)
}

func (p *parser) parseCall(name string) (Expr, error) {
	if _, err := p.expect(tokLParen, "("); err != nil {
		return nil, err
	}
	var args []Expr
	if p.peek().kind != tokRParen {
		for {
			a, err := p.parseOr()
			if err != nil {
				return nil, err
			}
			args = append(args, a)
			if p.peek().kind != tokComma {
				break
			}
			p.next()
		}
	}
	if _, err := p.expect(tokRParen, ")"); err != nil {
		return nil, err
	}
	return callExpr{name, args}, nil
}

func (p *parser) parsePath(root string) (Expr, error) {
	e := pathExpr{root: root}
	for {
		switch p.peek().kind {
		case tokDot:
			p.next()
			t, err := p.expect(tokIdent, "field name")
			if err != nil {
				return nil, err
			}
			e.steps = append(e.steps, pathStep{field: t.text})
		case tokLBracket:
			p.next()
			idx, err := p.parseOr()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tokRBracket, "]"); err != nil {
				return nil, err
			}
			e.steps = append(e.steps, pathStep{index: idx})
		default:
			return e, nil
		}
	}
}

type arrayExpr struct{ items []Expr }

func (e arrayExpr) Refs() [][]string {
	var out [][]string
	for _, item := range e.items {
		out = append(out, item.Refs()...)
	}
	return out
}

func (e arrayExpr) Eval(ctx map[string]any) (any, error) {
	out := make([]any, 0, len(e.items))
	for _, item := range e.items {
		v, err := item.Eval(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// ---------- value helpers ----------

func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case float64:
		return x != 0
	case string:
		return x != ""
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	default:
		return true
	}
}

func number(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	}
	return 0, false
}

func equal(a, b any) bool {
	if an, ok := number(a); ok {
		if bn, ok := number(b); ok {
			return an == bn
		}
	}
	return reflect.DeepEqual(a, b)
}

func compare(op string, a, b any) (bool, error) {
	if an, ok := number(a); ok {
		if bn, ok := number(b); ok {
			switch op {
			case "<":
				return an < bn, nil
			case "<=":
				return an <= bn, nil
			case ">":
				return an > bn, nil
			default:
				return an >= bn, nil
			}
		}
	}
	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		switch op {
		case "<":
			return as < bs, nil
		case "<=":
			return as <= bs, nil
		case ">":
			return as > bs, nil
		default:
			return as >= bs, nil
		}
	}
	return false, fmt.Errorf("cannot compare %T and %T", a, b)
}

func containsValue(container, needle any) bool {
	switch c := container.(type) {
	case string:
		return strings.Contains(c, stringify(needle))
	case []any:
		for _, item := range c {
			if equal(item, needle) {
				return true
			}
		}
		return false
	case map[string]any:
		_, ok := c[stringify(needle)]
		return ok
	}
	return false
}

func stringify(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		if x == math.Trunc(x) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return fmt.Sprint(x)
		}
		if len(b) > 0 && b[0] == '"' {
			var s string
			if json.Unmarshal(b, &s) == nil {
				return s
			}
		}
		return string(b)
	}
}
