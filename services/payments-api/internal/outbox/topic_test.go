package outbox

import "testing"

func TestTopicFor(t *testing.T) {
	cases := []struct {
		name  string
		topic string
		region string
		want  string
	}{
		{"empty region keeps the legacy topic", "payments.events.v1", "", "payments.events.v1"},
		{"default region keeps the legacy topic", "payments.events.v1", "A", "payments.events.v1"},
		{"other region qualifies the topic", "payments.events.v1", "B", "payments.events.v1.B"},
		{"other region qualifies a custom base", "events.custom", "C", "events.custom.C"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := TopicFor(tc.topic, tc.region); got != tc.want {
				t.Fatalf("TopicFor(%q, %q) = %q; want %q", tc.topic, tc.region, got, tc.want)
			}
		})
	}
}

func TestTopicDLQFor(t *testing.T) {
	if got := TopicDLQFor("payments.events.v1", ""); got != "payments.events.v1.dlq" {
		t.Fatalf("TopicDLQFor default = %q; want %q", got, "payments.events.v1.dlq")
	}
	if got := TopicDLQFor("payments.events.v1", "B"); got != "payments.events.v1.B.dlq" {
		t.Fatalf("TopicDLQFor region B = %q; want %q", got, "payments.events.v1.B.dlq")
	}
}
