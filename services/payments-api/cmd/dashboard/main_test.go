package main

import (
	"strings"
	"testing"
)

func TestParseAmountCents(t *testing.T) {
	tests := []struct {
		input string
		want  int64
		valid bool
	}{
		{input: "50", want: 5000, valid: true},
		{input: "50.0", want: 5000, valid: true},
		{input: "0.05", want: 5, valid: true},
		{input: "1.99", want: 199, valid: true},
		{input: "0", want: 0, valid: true},
		{input: "", valid: false},
		{input: ".50", valid: false},
		{input: "1.234", valid: false},
		{input: "1,25", valid: false},
		{input: "-1", valid: false},
		{input: "92233720368547758.08", valid: false},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := parseAmountCents(test.input)
			if test.valid && err != nil {
				t.Fatalf("parseAmountCents(%q) returned error: %v", test.input, err)
			}
			if !test.valid && err == nil {
				t.Fatalf("parseAmountCents(%q) should reject input", test.input)
			}
			if test.valid && got != test.want {
				t.Errorf("parseAmountCents(%q) = %d, want %d", test.input, got, test.want)
			}
		})
	}
}

func TestPayResultEscapesUntrustedContent(t *testing.T) {
	result := payResultHTML(false, "", `<script>alert("x")</script>`)
	if result == "" {
		t.Fatal("expected error result")
	}
	if want := "&lt;script&gt;"; !strings.Contains(result, want) {
		t.Errorf("result does not escape markup: %s", result)
	}
	if strings.Contains(result, "<script>") {
		t.Errorf("result contains executable markup: %s", result)
	}
}
