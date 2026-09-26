package sdl_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/go-apis/loom/sdl"
)

const enumHome = "service orders\n\nenum Priority { low normal }\n"

func enumFiles(t *testing.T, files ...sdl.File) []string {
	t.Helper()
	s, err := sdl.ParseFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	e := s.FindEnum("Priority")
	if e == nil {
		t.Fatal("no Priority")
	}
	return e.Values
}

func TestEnumExtendAcrossFilesInPathOrder(t *testing.T) {
	files := []sdl.File{
		{Path: "a.loom", Src: enumHome},
		{Path: "b.loom", Src: "enum Priority += { high }\n"},
		{Path: "c.loom", Src: "enum Priority += { urgent critical }\n"},
	}
	want := []string{"low", "normal", "high", "urgent", "critical"}
	for i := 0; i < 3; i++ {
		if got := enumFiles(t, files...); !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestEnumExtendBeforeHomeInSourceStillAfterHomeValues(t *testing.T) {
	got := enumFiles(t,
		sdl.File{Path: "a.loom", Src: "service orders\nenum Priority += { high }\n"},
		sdl.File{Path: "b.loom", Src: "enum Priority { low }\n"})
	if want := []string{"low", "high"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEnumExtendRefusals(t *testing.T) {
	cases := []struct {
		name  string
		files []sdl.File
		want  string
	}{
		{"no home", []sdl.File{
			{Path: "a.loom", Src: "service orders\n"},
			{Path: "b.loom", Src: "\nenum Priority += { high }\n"}},
			"b.loom:2: enum Priority += … extends an undeclared enum"},
		{"two homes", []sdl.File{
			{Path: "a.loom", Src: enumHome},
			{Path: "b.loom", Src: "\n\nenum Priority { high }\n"}},
			"b.loom:3: enum Priority is already declared at a.loom:3"},
		{"duplicate value", []sdl.File{
			{Path: "a.loom", Src: enumHome},
			{Path: "b.loom", Src: "enum Priority += {\n  high\n  normal\n}\n"}},
			"b.loom:3: enum Priority declares value normal twice: also at a.loom:3"},
		{"duplicate between extensions", []sdl.File{
			{Path: "a.loom", Src: enumHome},
			{Path: "b.loom", Src: "enum Priority += { high }\n"},
			{Path: "c.loom", Src: "enum Priority += { high }\n"}},
			"c.loom:1: enum Priority declares value high twice: also at b.loom:1"},
		{"empty extension", []sdl.File{
			{Path: "a.loom", Src: enumHome},
			{Path: "b.loom", Src: "enum Priority += { }\n"}},
			"b.loom:1: enum Priority += has no values"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := sdl.ParseFiles(c.files)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want it to contain %q", err, c.want)
			}
		})
	}
}

func TestEnumExtendKeepsValidateChecks(t *testing.T) {
	_, err := sdl.ParseFiles([]sdl.File{
		{Path: "a.loom", Src: "service orders\ntype Priority { n: int }\nenum Priority { low }\n"},
		{Path: "b.loom", Src: "enum Priority += { high }\n"}})
	if err == nil || !strings.Contains(err.Error(), "collides with type") {
		t.Fatalf("got %v", err)
	}
	_, err = sdl.ParseFiles([]sdl.File{
		{Path: "a.loom", Src: "service orders\nenum Priority { low low }\n"},
		{Path: "b.loom", Src: "enum Priority += { high }\n"}})
	if err == nil || !strings.Contains(err.Error(), "declares value low twice") {
		t.Fatalf("got %v", err)
	}
	_, err = sdl.ParseFiles([]sdl.File{
		{Path: "a.loom", Src: enumHome},
		{Path: "b.loom", Src: "enum Priority += { bad-name }\n"}})
	if err == nil {
		t.Fatal("want error")
	}
}

func TestEnumWithoutExtensionUnchanged(t *testing.T) {
	s, err := sdl.Parse("service orders\nenum Priority { low normal }\nenum Kind { a b }\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Enums) != 2 || !reflect.DeepEqual(s.FindEnum("Priority").Values, []string{"low", "normal"}) {
		t.Fatalf("%+v", s.Enums)
	}
	_, err = sdl.Parse("service orders\nenum P { a }\nenum P { b }\n")
	if err == nil || !strings.Contains(err.Error(), "line 3: enum P is already declared at line 2") {
		t.Fatalf("got %v", err)
	}
}
