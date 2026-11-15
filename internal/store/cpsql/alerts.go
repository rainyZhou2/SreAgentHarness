package cpsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go-sre-harness/internal/alerting"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/work"
)

type alertRowQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) ProjectAlert(ctx context.Context, in alerting.Input) (alerting.Projection, error) {
	if err := validateAlertInput(&in); err != nil {
		return alerting.Projection{}, err
	}
	for attempt := 0; attempt < 4; attempt++ {
		var out alerting.Projection
		err := s.tx(ctx, func(tx *sql.Tx) error {
			var err error
			out, err = s.projectAlertTx(ctx, tx, in)
			return err
		})
		if err == nil {
			return out, nil
		}
		if !isUnique(err, "") || ctx.Err() != nil {
			return alerting.Projection{}, err
		}
	}
	return alerting.Projection{}, fmt.Errorf("alert projection did not converge after bounded conflict retries")
}

func validateAlertInput(in *alerting.Input) error {
	if err := in.Policy.Normalize(); err != nil {
		return err
	}
	if in.InboxID == "" || in.TenantID == "" || in.AlertName == "" || in.SubjectRef == "" || in.SymptomKey == "" || in.OccurredAt.IsZero() {
		return fmt.Errorf("%w: incomplete alert identity", alerting.ErrInvalid)
	}
	if in.EventType != alerting.EventFiring && in.EventType != alerting.EventResolved {
		return fmt.Errorf("%w: unsupported event type %q", alerting.ErrInvalid, in.EventType)
	}
	if in.EventType == alerting.EventFiring && in.Fingerprint == "" {
		return fmt.Errorf("%w: firing fingerprint is required", alerting.ErrInvalid)
	}
	if in.ObservedAt.IsZero() {
		in.ObservedAt = in.OccurredAt
	}
	return nil
}

func (s *Store) projectAlertTx(ctx context.Context, tx *sql.Tx, in alerting.Input) (alerting.Projection, error) {
	if existing, ok, err := s.alertInstanceByInbox(ctx, tx, in.TenantID, in.InboxID); err != nil {
		return alerting.Projection{}, err
	} else if ok {
		return s.projectionFromInstance(ctx, tx, existing)
	}

	symptom, err := s.lockSymptom(ctx, tx, in)
	if err != nil {
		return alerting.Projection{}, err
	}
	instance := alerting.Instance{ID: cpkit.NewID(), TenantID: in.TenantID, InboxID: in.InboxID, SymptomID: symptom.ID,
		Fingerprint: in.Fingerprint, AlertName: in.AlertName, SubjectRef: in.SubjectRef, SymptomKind: in.SymptomKind,
		ImpactScope: in.ImpactScope, Severity: in.Severity, EventType: in.EventType, Labels: in.Labels,
		OccurredAt: in.OccurredAt, ObservedAt: in.ObservedAt, NotificationSuppressed: in.NotificationSuppressed,
		SuppressionReasons: in.SuppressionReasons}

	var episode *alerting.Episode
	changed := false
	if in.EventType == alerting.EventFiring {
		episode, instance.Resolution, changed, err = s.applyFiring(ctx, tx, in, &symptom)
	} else {
		episode, instance.Resolution, changed, err = s.applyResolved(ctx, tx, in, &symptom)
	}
	if err != nil {
		return alerting.Projection{}, err
	}
	if episode != nil && episode.SymptomID != symptom.ID {
		return alerting.Projection{}, fmt.Errorf("%w: fingerprint %q belongs to a different Symptom", alerting.ErrInvalid, in.Fingerprint)
	}
	if episode != nil {
		instance.EpisodeID = episode.ID
	}
	if changed {
		if in.OccurredAt.After(symptom.WindowEnd) {
			symptom.WindowEnd = in.OccurredAt
		}
		if err := s.updateSymptom(ctx, tx, &symptom); err != nil {
			return alerting.Projection{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO alert_instances (tenant_id, id, inbox_id, episode_id, symptom_id,
		fingerprint, alert_name, subject_ref, event_type, occurred_at, observed_at, record, created_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`), in.TenantID, instance.ID, in.InboxID, nullStr(instance.EpisodeID), symptom.ID,
		nullStr(in.Fingerprint), in.AlertName, in.SubjectRef, in.EventType, s.t(in.OccurredAt), s.t(in.ObservedAt),
		js(instance), s.t(in.ObservedAt)); err != nil {
		return alerting.Projection{}, err
	}
	return alerting.Projection{Instance: instance, Episode: episode, Symptom: symptom}, nil
}

func (s *Store) lockSymptom(ctx context.Context, tx *sql.Tx, in alerting.Input) (alerting.Symptom, error) {
	symptom, err := scanAlertSymptom(tx.QueryRowContext(ctx, s.q(`SELECT record FROM alert_symptoms
		WHERE tenant_id=? AND symptom_key=?`+s.forUpdate()), in.TenantID, in.SymptomKey))
	if err == nil {
		return symptom, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return alerting.Symptom{}, err
	}
	symptom = alerting.Symptom{ID: cpkit.NewID(), TenantID: in.TenantID, Key: in.SymptomKey, SubjectRef: in.SubjectRef,
		Kind: in.SymptomKind, ImpactScope: in.ImpactScope, State: alerting.StateResolved,
		WindowStart: in.OccurredAt, WindowEnd: in.OccurredAt, Version: 1}
	_, err = tx.ExecContext(ctx, s.q(`INSERT INTO alert_symptoms (tenant_id, id, symptom_key, subject_ref, kind,
		impact_scope, state, window_start, window_end, active_episodes, version, record) VALUES (?,?,?,?,?,?,?,?,?,0,?,?)`),
		symptom.TenantID, symptom.ID, symptom.Key, symptom.SubjectRef, symptom.Kind, symptom.ImpactScope, symptom.State,
		s.t(symptom.WindowStart), s.t(symptom.WindowEnd), symptom.Version, js(symptom))
	return symptom, err
}

func (s *Store) applyFiring(ctx context.Context, tx *sql.Tx, in alerting.Input, symptom *alerting.Symptom) (*alerting.Episode, string, bool, error) {
	episode, err := scanAlertEpisode(tx.QueryRowContext(ctx, s.q(`SELECT record FROM alert_episodes
		WHERE tenant_id=? AND fingerprint=? AND state='FIRING'`+s.forUpdate()), in.TenantID, in.Fingerprint))
	if err == nil {
		if in.OccurredAt.Before(episode.LastEventAt) {
			return &episode, alerting.ResolutionOutOfOrder, false, nil
		}
		episode.LastEventAt = in.OccurredAt
		return &episode, "", true, s.updateEpisode(ctx, tx, &episode)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, "", false, err
	}

	latest, err := scanAlertEpisode(tx.QueryRowContext(ctx, s.q(`SELECT record FROM alert_episodes
		WHERE tenant_id=? AND fingerprint=? AND state='RESOLVED' ORDER BY last_event_at DESC LIMIT 1`+s.forUpdate()),
		in.TenantID, in.Fingerprint))
	reopen := err == nil && !in.OccurredAt.Before(latest.LastEventAt) && latest.ResolvedAt != nil &&
		in.OccurredAt.Sub(*latest.ResolvedAt) <= in.Policy.ReopenWindow.D()
	if err == nil && in.OccurredAt.Before(latest.LastEventAt) {
		return &latest, alerting.ResolutionOutOfOrder, false, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, "", false, err
	}
	if reopen {
		latest.State = alerting.StateFiring
		latest.LastEventAt = in.OccurredAt
		latest.ResolvedAt = nil
		latest.ReopenCount++
		if latest.LastReopenAt == nil || in.OccurredAt.Sub(*latest.LastReopenAt) > in.Policy.FlappingWindow.D() {
			latest.FlappingCount = 1
		} else {
			latest.FlappingCount++
		}
		lastReopenAt := in.OccurredAt
		latest.LastReopenAt = &lastReopenAt
		latest.Flapping = latest.FlappingCount >= in.Policy.FlappingThreshold
		if err := s.updateEpisode(ctx, tx, &latest); err != nil {
			return nil, "", false, err
		}
		activateSymptom(symptom, in.OccurredAt)
		return &latest, "", true, nil
	}

	episode = alerting.Episode{ID: cpkit.NewID(), TenantID: in.TenantID, Fingerprint: in.Fingerprint,
		AlertName: in.AlertName, SubjectRef: in.SubjectRef, State: alerting.StateFiring, SymptomID: symptom.ID,
		OpenedAt: in.OccurredAt, LastEventAt: in.OccurredAt, Version: 1}
	if err := s.insertEpisode(ctx, tx, episode); err != nil {
		return nil, "", false, err
	}
	activateSymptom(symptom, in.OccurredAt)
	return &episode, "", true, nil
}

func (s *Store) applyResolved(ctx context.Context, tx *sql.Tx, in alerting.Input, symptom *alerting.Symptom) (*alerting.Episode, string, bool, error) {
	var episode alerting.Episode
	var err error
	if in.Fingerprint != "" {
		episode, err = scanAlertEpisode(tx.QueryRowContext(ctx, s.q(`SELECT record FROM alert_episodes
			WHERE tenant_id=? AND fingerprint=? AND state='FIRING'`+s.forUpdate()), in.TenantID, in.Fingerprint))
	} else {
		episode, err = uniqueResolutionCandidate(ctx, tx, s, in)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, alerting.ResolutionAmbiguous, false, nil
	}
	if err != nil {
		return nil, "", false, err
	}
	if in.OccurredAt.Before(episode.LastEventAt) {
		return &episode, alerting.ResolutionOutOfOrder, false, nil
	}
	episode.State = alerting.StateResolved
	episode.LastEventAt = in.OccurredAt
	resolvedAt := in.OccurredAt
	episode.ResolvedAt = &resolvedAt
	if err := s.updateEpisode(ctx, tx, &episode); err != nil {
		return nil, "", false, err
	}
	if symptom.ActiveEpisodes <= 0 {
		return nil, "", false, fmt.Errorf("alert symptom %s has no active Episode to resolve", symptom.ID)
	}
	symptom.ActiveEpisodes--
	if symptom.ActiveEpisodes == 0 {
		symptom.State = alerting.StateResolved
	}
	return &episode, alerting.ResolutionMatched, true, nil
}

func uniqueResolutionCandidate(ctx context.Context, tx *sql.Tx, s *Store, in alerting.Input) (alerting.Episode, error) {
	rows, err := tx.QueryContext(ctx, s.q(`SELECT record FROM alert_episodes WHERE tenant_id=? AND alert_name=?
		AND subject_ref=? AND state='FIRING' ORDER BY last_event_at DESC LIMIT 2`+s.forUpdate()), in.TenantID, in.AlertName, in.SubjectRef)
	if err != nil {
		return alerting.Episode{}, err
	}
	defer rows.Close()
	var candidates []alerting.Episode
	for rows.Next() {
		episode, err := scanAlertEpisode(rows)
		if err != nil {
			return alerting.Episode{}, err
		}
		candidates = append(candidates, episode)
	}
	if err := rows.Err(); err != nil {
		return alerting.Episode{}, err
	}
	if len(candidates) != 1 {
		return alerting.Episode{}, sql.ErrNoRows
	}
	return candidates[0], nil
}

func activateSymptom(symptom *alerting.Symptom, at time.Time) {
	if symptom.ActiveEpisodes == 0 {
		symptom.WindowStart = at
	}
	symptom.ActiveEpisodes++
	symptom.State = alerting.StateFiring
}

func (s *Store) insertEpisode(ctx context.Context, tx *sql.Tx, episode alerting.Episode) error {
	_, err := tx.ExecContext(ctx, s.q(`INSERT INTO alert_episodes (tenant_id, id, fingerprint, alert_name, subject_ref,
		state, symptom_id, opened_at, last_event_at, resolved_at, reopen_count, flapping, version, record)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`), episode.TenantID, episode.ID, episode.Fingerprint, episode.AlertName,
		episode.SubjectRef, episode.State, episode.SymptomID, s.t(episode.OpenedAt), s.t(episode.LastEventAt),
		s.tp(episode.ResolvedAt), episode.ReopenCount, episode.Flapping, episode.Version, js(episode))
	return err
}

func (s *Store) updateEpisode(ctx context.Context, tx *sql.Tx, episode *alerting.Episode) error {
	expected := episode.Version
	episode.Version++
	res, err := tx.ExecContext(ctx, s.q(`UPDATE alert_episodes SET state=?, last_event_at=?, resolved_at=?, reopen_count=?,
		flapping=?, version=?, record=? WHERE tenant_id=? AND id=? AND version=?`), episode.State, s.t(episode.LastEventAt),
		s.tp(episode.ResolvedAt), episode.ReopenCount, episode.Flapping, episode.Version, js(episode),
		episode.TenantID, episode.ID, expected)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err == nil && n != 1 {
		return work.ErrVersionConflict
	}
	return err
}

func (s *Store) updateSymptom(ctx context.Context, tx *sql.Tx, symptom *alerting.Symptom) error {
	expected := symptom.Version
	symptom.Version++
	res, err := tx.ExecContext(ctx, s.q(`UPDATE alert_symptoms SET state=?, window_start=?, window_end=?, active_episodes=?,
		version=?, record=? WHERE tenant_id=? AND id=? AND version=?`), symptom.State, s.t(symptom.WindowStart),
		s.t(symptom.WindowEnd), symptom.ActiveEpisodes, symptom.Version, js(symptom), symptom.TenantID, symptom.ID, expected)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err == nil && n != 1 {
		return work.ErrVersionConflict
	}
	return err
}

func (s *Store) alertInstanceByInbox(ctx context.Context, q alertRowQuerier, tenantID, inboxID string) (alerting.Instance, bool, error) {
	var body jb
	err := q.QueryRowContext(ctx, s.q(`SELECT record FROM alert_instances WHERE tenant_id=? AND inbox_id=?`), tenantID, inboxID).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return alerting.Instance{}, false, nil
	}
	if err != nil {
		return alerting.Instance{}, false, err
	}
	var instance alerting.Instance
	return instance, true, json.Unmarshal(body, &instance)
}

func (s *Store) projectionFromInstance(ctx context.Context, q alertRowQuerier, instance alerting.Instance) (alerting.Projection, error) {
	out := alerting.Projection{Instance: instance}
	var err error
	out.Symptom, err = scanAlertSymptom(q.QueryRowContext(ctx, s.q(`SELECT record FROM alert_symptoms WHERE tenant_id=? AND id=?`),
		instance.TenantID, instance.SymptomID))
	if err != nil {
		return alerting.Projection{}, err
	}
	if instance.EpisodeID != "" {
		episode, err := scanAlertEpisode(q.QueryRowContext(ctx, s.q(`SELECT record FROM alert_episodes WHERE tenant_id=? AND id=?`),
			instance.TenantID, instance.EpisodeID))
		if err != nil {
			return alerting.Projection{}, err
		}
		out.Episode = &episode
	}
	return out, nil
}

func (s *Store) AlertProjection(ctx context.Context, tenantID, inboxID string) (alerting.Projection, error) {
	instance, ok, err := s.alertInstanceByInbox(ctx, s.db, tenantID, inboxID)
	if err != nil {
		return alerting.Projection{}, err
	}
	if !ok {
		return alerting.Projection{}, work.ErrNotFound
	}
	return s.projectionFromInstance(ctx, s.db, instance)
}

func (s *Store) AlertEpisode(ctx context.Context, tenantID, episodeID string) (alerting.Episode, error) {
	episode, err := scanAlertEpisode(s.db.QueryRowContext(ctx, s.q(`SELECT record FROM alert_episodes WHERE tenant_id=? AND id=?`),
		tenantID, episodeID))
	if errors.Is(err, sql.ErrNoRows) {
		return alerting.Episode{}, work.ErrNotFound
	}
	return episode, err
}

func (s *Store) AlertSymptom(ctx context.Context, tenantID, symptomID string) (alerting.Symptom, error) {
	symptom, err := scanAlertSymptom(s.db.QueryRowContext(ctx, s.q(`SELECT record FROM alert_symptoms WHERE tenant_id=? AND id=?`),
		tenantID, symptomID))
	if errors.Is(err, sql.ErrNoRows) {
		return alerting.Symptom{}, work.ErrNotFound
	}
	return symptom, err
}

func scanAlertEpisode(r rowScanner) (alerting.Episode, error) {
	var body jb
	var episode alerting.Episode
	if err := r.Scan(&body); err != nil {
		return episode, err
	}
	return episode, json.Unmarshal(body, &episode)
}

func scanAlertSymptom(r rowScanner) (alerting.Symptom, error) {
	var body jb
	var symptom alerting.Symptom
	if err := r.Scan(&body); err != nil {
		return symptom, err
	}
	return symptom, json.Unmarshal(body, &symptom)
}
