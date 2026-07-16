package extract

import (
	"go/types"
	"reflect"
	"strings"
)

// embeddedTag finds an embedded field named fieldName (promoted through
// nested anonymous structs, like reflect's FieldByName) and returns its
// `es:"..."` tag.
func (e *extractor) embeddedTag(n *types.Named, fieldName string) (string, bool) {
	return embeddedTagIn(n, fieldName, map[*types.Named]bool{})
}

func embeddedTagIn(n *types.Named, fieldName string, seen map[*types.Named]bool) (string, bool) {
	if n == nil || seen[n] {
		return "", false
	}
	seen[n] = true
	st, ok := n.Underlying().(*types.Struct)
	if !ok {
		return "", false
	}
	// direct match first, then promoted — reflect.FieldByName's shallowest-
	// depth rule collapses to this for the one level we care about.
	for i := 0; i < st.NumFields(); i++ {
		f := st.Field(i)
		if f.Anonymous() && f.Name() == fieldName {
			return reflect.StructTag(st.Tag(i)).Get("es"), true
		}
	}
	for i := 0; i < st.NumFields(); i++ {
		f := st.Field(i)
		if !f.Anonymous() {
			continue
		}
		if tag, ok := embeddedTagIn(concreteNamed(f.Type()), fieldName, seen); ok {
			return tag, ok
		}
	}
	return "", false
}

// splitTag mirrors utils.SplitTag's `;` separation.
func splitTag(tag string) []string {
	if tag == "" {
		return nil
	}
	items := strings.Split(tag, ";")
	for i := range items {
		items[i] = strings.TrimSpace(items[i])
	}
	return items
}

// cutTag splits one tag item on `=`, mirroring the strings.Split usage in
// config_event.go et al.
func cutTag(item string) (key, val string, hasVal bool) {
	key, val, hasVal = strings.Cut(item, "=")
	return key, val, hasVal
}

func splitComma(s string) []string {
	return strings.Split(s, ",")
}

// isTrue mirrors es.IsTrue.
func isTrue(s string) bool {
	for _, v := range []string{"true", "1", "yes", "y", "t"} {
		if strings.EqualFold(s, v) {
			return true
		}
	}
	return false
}
