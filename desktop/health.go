package main

// Ping verifies the control plane still answers the authenticated caller.
func (a *App) Ping() error {
	c, err := a.c()
	if err != nil {
		return err
	}
	var out map[string]any
	return c.Get(a.ctx, "/api/whoami", &out)
}
