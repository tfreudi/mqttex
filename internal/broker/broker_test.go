package broker

import "testing"

func TestMatches(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"#", "a/b", true},
		{"#", "$SYS/x", false},
		{"+/x", "$SYS/x", false},
		{"/topic/SN1/#", "/topic/SN1/a/b", true},
		{"/topic/SN1/#", "/topic/SN1", true},
		{"/topic/SN1/#", "/topic/SN10/a", false},
		{"/topic/+/a", "/topic/SN1/a", true},
		{"/topic/+/a", "/topic/SN1/b", false},
		{"a/+", "a", false},
		{"a/+", "a/", true},
		{"a/b", "a/b", true},
		{"a/b", "a/b/c", false},
		{"a/b/c", "a/b", false},
	}
	for _, c := range cases {
		if got := matches(c.filter, c.topic); got != c.want {
			t.Errorf("matches(%q, %q) = %v, want %v", c.filter, c.topic, got, c.want)
		}
	}
	if !matchesAny([]string{"x/#", "a/b"}, "a/b") || matchesAny(nil, "a") {
		t.Error("matchesAny")
	}
}
