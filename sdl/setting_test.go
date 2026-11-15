package sdl_test

import (
	"strings"
	"testing"

	"github.com/go-apis/loom/sdl"
)

// TestSettingDeclaration: a setting has a name, a type and a default,
// its doc comment, and the file that declares it. What does not make
// sense is refused: a type that is no scalar and no enum, a default that
// is not of the type, an enum setting with no default or one outside the
// enum, a setting declared twice, and a directive settings do not take.
func TestSettingDeclaration(t *testing.T) {
	s, err := sdl.ParseFiles([]sdl.File{{Path: "schema/caps.loom", Src: `service shop

enum Tier { gold silver }

/// How many builds run at once.
setting build_cap: int @default(2)
setting tier: Tier @default(silver)
setting paused: bool
setting greeting: string @default("hello there")
`}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	if len(s.Settings) != 4 {
		t.Fatalf("settings: %+v", s.Settings)
	}
	cap := s.Settings[0]
	if cap.Name != "build_cap" || cap.Type != "int" || cap.Default != "2" || !cap.HasDefault || cap.Doc != "How many builds run at once." || cap.File != "schema/caps.loom" {
		t.Errorf("build_cap: %+v", cap)
	}
	if v, err := cap.DefaultValue(); err != nil || v != int64(2) {
		t.Errorf("build_cap's default: %v %v", v, err)
	}
	for _, st := range s.Settings {
		if st.Name == "paused" {
			if v, err := st.DefaultValue(); err != nil || v != false || st.HasDefault {
				t.Errorf("paused, which has no default: %v %v", v, err)
			}
		}
		if st.Name == "greeting" && st.Default != "hello there" {
			t.Errorf("greeting's default: %q", st.Default)
		}
	}

	for name, c := range map[string]struct{ src, want string }{
		"a type that is neither":    {"setting x: Colour @default(red)", "type Colour is not string, int, bool, float or an enum"},
		"a word for an int":         {"setting x: int @default(many)", "@default(many) is not an int"},
		"a number for a bool":       {"setting x: bool @default(1)", "@default(1) is not true or false"},
		"an enum value not in it":   {"enum Tier { gold }\nsetting x: Tier @default(bronze)", "@default(bronze) is not a value of Tier"},
		"an enum with no default":   {"enum Tier { gold }\nsetting x: Tier", "an enum setting needs a @default"},
		"declared twice":            {"setting x: int\nsetting x: int", "setting x is declared twice"},
		"a directive settings lack": {"setting x: int @retain(30d)", "settings take @default(<value>)"},
		"no type":                   {"setting x", "expected `: <type>`"},
	} {
		s, err := sdl.Parse("service shop\n\n" + c.src + "\n")
		if err == nil {
			err = s.Validate()
		}
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error saying %q", name, err, c.want)
		}
	}
}
