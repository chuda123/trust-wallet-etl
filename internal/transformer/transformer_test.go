package transformer_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/chuda123/trust-wallet-etl/internal/transformer"
)

const sample = `{
  "gender": "male",
  "name": {"title": "Mr", "first": "Harry", "last": "Burton"},
  "location": {
    "street": {"number": 3993, "name": "W Dallas St"},
    "city": "Boise", "state": "Texas", "country": "United States",
    "postcode": 61557,
    "coordinates": {"latitude": "-73.2206", "longitude": "-106.7530"},
    "timezone": {"offset": "-11:00", "description": "Midway Island, Samoa"}
  },
  "email": "Harry.Burton@example.com",
  "login": {
    "uuid": "6106e1d9-dfea-45ae-8b0b-320e9861a898",
    "username": "angrypanda314",
    "password": "qwe123",
    "salt": "186NtP4d",
    "md5": "deadbeef",
    "sha1": "deadbeef",
    "sha256": "deadbeef"
  },
  "dob": {"date": "1951-10-14T04:21:30.137Z", "age": 74},
  "registered": {"date": "2002-09-22T08:20:58.921Z", "age": 23},
  "phone": "(653) 265-5591",
  "cell": "(412) 218-2946",
  "id": {"name": "SSN", "value": "798-51-5206"},
  "picture": {
    "large": "https://randomuser.me/api/portraits/men/92.jpg",
    "medium": "https://randomuser.me/api/portraits/med/men/92.jpg",
    "thumbnail": "https://randomuser.me/api/portraits/thumb/men/92.jpg"
  },
  "nat": "US"
}`

func TestTransformNormalizesAndDropsSensitiveFields(t *testing.T) {
	ingested := time.Date(2026, 9, 4, 13, 0, 0, 0, time.UTC)
	rec, err := transformer.Transform(json.RawMessage(sample), "randomuser", "1.4", "batch-1", ingested)
	if err != nil {
		t.Fatal(err)
	}

	if rec.Meta.SchemaVersion != transformer.SchemaVersion {
		t.Fatalf("schema_version: %s", rec.Meta.SchemaVersion)
	}
	if rec.Meta.IngestedAt != "2026-09-04T13:00:00Z" && rec.Meta.IngestedAt != "2026-09-04T13:00:00.000000000Z" {
		if !strings.HasPrefix(rec.Meta.IngestedAt, "2026-09-04T13:00:00") || !strings.HasSuffix(rec.Meta.IngestedAt, "Z") {
			t.Fatalf("ingested_at not UTC ISO-8601: %s", rec.Meta.IngestedAt)
		}
	}
	if rec.User.SourceUUID != "6106e1d9-dfea-45ae-8b0b-320e9861a898" {
		t.Fatalf("uuid: %s", rec.User.SourceUUID)
	}
	if rec.User.Email != "harry.burton@example.com" {
		t.Fatalf("email not normalized: %s", rec.User.Email)
	}
	if rec.User.Name.DisplayName != "Harry Burton" {
		t.Fatalf("display name: %s", rec.User.Name.DisplayName)
	}
	if rec.User.Location.PostalCode != "61557" {
		t.Fatalf("postcode: %s", rec.User.Location.PostalCode)
	}
	if rec.User.Location.Latitude == 0 || rec.User.Location.Longitude == 0 {
		t.Fatalf("coordinates not parsed: %+v", rec.User.Location)
	}
	if rec.User.Contact.Phone != "6532655591" {
		t.Fatalf("phone digits: %s", rec.User.Contact.Phone)
	}
	if !strings.HasSuffix(rec.User.BirthAt, "Z") {
		t.Fatalf("birth_at not UTC: %s", rec.User.BirthAt)
	}
	if rec.User.AvatarURL == "" || strings.Contains(rec.User.AvatarURL, "/med/") {
		t.Fatalf("expected thumbnail only, got %s", rec.User.AvatarURL)
	}

	encoded, _ := json.Marshal(rec)
	blob := string(encoded)
	for _, forbidden := range []string{"qwe123", "186NtP4d", "deadbeef", "798-51-5206", "Midway Island"} {
		if strings.Contains(blob, forbidden) {
			t.Fatalf("sensitive/dropped field leaked: %s", forbidden)
		}
	}
}

func TestTransformRejectsMissingNaturalKey(t *testing.T) {
	_, err := transformer.Transform(json.RawMessage(`{"login":{}}`), "randomuser", "1.4", "b", time.Now())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTransformAllIsolatesBadRecords(t *testing.T) {
	raw := []json.RawMessage{
		json.RawMessage(sample),
		json.RawMessage(`{"login":{}}`),
	}
	out := transformer.TransformAll(raw, "randomuser", "1.4", "b", time.Now().UTC())
	if len(out) != 2 {
		t.Fatalf("len=%d", len(out))
	}
	if out[0].Err != nil {
		t.Fatalf("first should succeed: %v", out[0].Err)
	}
	if out[1].Err == nil {
		t.Fatal("second should fail")
	}
}
