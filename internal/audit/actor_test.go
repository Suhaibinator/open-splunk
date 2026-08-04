package audit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestActorContextValidationAndDetachment(t *testing.T) {
	t.Parallel()

	valid := []Actor{
		{Kind: ActorKindSystem, ID: "open-splunk-server", Role: ActorRoleSystem},
		{Kind: ActorKindBrowser, ID: "ordinary-user", Role: ActorRoleUser},
		{Kind: ActorKindBrowser, ID: "administrator", Role: ActorRoleAdministrator},
	}
	for _, actor := range valid {
		ctx, err := WithActor(context.Background(), actor)
		if err != nil {
			t.Fatalf("WithActor(%+v): %v", actor, err)
		}
		got, ok := ActorFromContext(ctx)
		if !ok || got != actor || !got.Valid() {
			t.Fatalf("ActorFromContext() = (%+v, %t), want %+v", got, ok, actor)
		}
	}

	invalidUTF8 := string([]byte{0xff})
	invalid := []Actor{
		{},
		{Kind: ActorKind("unknown"), ID: "actor", Role: ActorRoleSystem},
		{Kind: ActorKindSystem, ID: "actor", Role: ActorRoleAdministrator},
		{Kind: ActorKindBrowser, ID: "actor", Role: ActorRoleSystem},
		{Kind: ActorKindBrowser, ID: " actor", Role: ActorRoleUser},
		{Kind: ActorKindBrowser, ID: "actor\n", Role: ActorRoleUser},
		{Kind: ActorKindBrowser, ID: invalidUTF8, Role: ActorRoleUser},
		{Kind: ActorKindBrowser, ID: strings.Repeat("a", maximumActorIDBytes+1), Role: ActorRoleUser},
	}
	for _, actor := range invalid {
		if actor.Valid() {
			t.Fatalf("invalid actor reported valid: %+v", actor)
		}
		ctx, err := WithActor(context.Background(), actor)
		if ctx != nil || !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("WithActor(%+v) = (%v, %v), want nil/invalid", actor, ctx, err)
		}
	}
	//nolint:staticcheck // Explicitly verifies the exported nil-context guard.
	if ctx, err := WithActor(nil, valid[0]); ctx != nil ||
		!errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("WithActor(nil) = (%v, %v), want nil/invalid", ctx, err)
	}
	//nolint:staticcheck // Explicitly verifies the exported nil-context guard.
	if actor, ok := ActorFromContext(nil); ok || actor != (Actor{}) {
		t.Fatalf("ActorFromContext(nil) = (%+v, %t)", actor, ok)
	}
	if actor, ok := ActorFromContext(context.Background()); ok || actor != (Actor{}) {
		t.Fatalf("ActorFromContext(empty) = (%+v, %t)", actor, ok)
	}
	forged := context.WithValue(
		context.Background(),
		actorContextKey{},
		Actor{Kind: ActorKindBrowser, ID: "bad\nactor", Role: ActorRoleUser},
	)
	if actor, ok := ActorFromContext(forged); ok || actor != (Actor{}) {
		t.Fatalf("ActorFromContext(forged) = (%+v, %t)", actor, ok)
	}
}

func TestEventValidateForTenantEnforcesPersistedTaxonomy(t *testing.T) {
	t.Parallel()
	valid := Event{
		Sequence:   1,
		TenantID:   "tenant",
		OccurredAt: time.UnixMicro(auditTestTime.UnixMicro()).UTC(),
		Actor: Actor{
			Kind: ActorKindSystem, ID: defaultSystemActorID, Role: ActorRoleSystem,
		},
		Action:        ActionIngestionTokenCreate,
		TargetKind:    TargetKindIngestionToken,
		TargetID:      "token",
		TargetVersion: 1,
	}
	if err := valid.ValidateForTenant("tenant"); err != nil {
		t.Fatalf("ValidateForTenant(valid): %v", err)
	}
	mutations := []func(*Event){
		func(event *Event) { event.Sequence = 0 },
		func(event *Event) { event.TenantID = "other" },
		func(event *Event) { event.OccurredAt = event.OccurredAt.Add(time.Nanosecond) },
		func(event *Event) { event.OccurredAt = event.OccurredAt.In(time.FixedZone("UTC-copy", 0)) },
		func(event *Event) {
			event.Actor = Actor{Kind: ActorKindBrowser, ID: "user", Role: ActorRoleUser}
		},
		func(event *Event) { event.Action = ActionIngestionTokenUpdate },
		func(event *Event) { event.TargetID = " bad" },
	}
	for index, mutate := range mutations {
		candidate := valid
		mutate(&candidate)
		if err := candidate.ValidateForTenant("tenant"); !errors.Is(err, control.ErrInvalidArgument) {
			t.Errorf("mutation %d error = %v", index, err)
		}
	}
}
