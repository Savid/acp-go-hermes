package hermesacp

import (
	"fmt"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"
)

// appendHostListedModels publishes the ids the host listed explicitly after
// the native rows, each under the group its provider prefix names and as the
// id alone. A native row of the same value stands and the host entry adds
// nothing. With no native row selected, the first host-listed id is current.
func appendHostListedModels(
	groups acp.SessionConfigSelectOptionsGrouped,
	hostListed []string,
	current *string,
) acp.SessionConfigSelectOptionsGrouped {
	for _, id := range hostListed {
		providerID, _, _ := strings.Cut(id, "/")

		index := slices.IndexFunc(groups, func(group acp.SessionConfigSelectGroup) bool {
			return string(group.Group) == providerID
		})
		if index < 0 {
			groups = append(groups, acp.SessionConfigSelectGroup{
				Group: acp.SessionConfigGroupId(providerID),
				Name:  providerID,
			})
			index = len(groups) - 1
		}

		if slices.ContainsFunc(groups[index].Options, func(option acp.SessionConfigSelectOption) bool {
			return string(option.Value) == id
		}) {
			continue
		}

		if *current == "" {
			*current = id
		}

		groups[index].Options = append(groups[index].Options, acp.SessionConfigSelectOption{
			Name:  id,
			Value: acp.SessionConfigValueId(id),
		})
	}

	return groups
}

// validateConfiguredModels refuses an id that could not name a model here:
// empty, carrying surrounding space, missing its <provider>/ prefix, or listed
// twice.
func validateConfiguredModels(ids []string) error {
	seen := make(map[string]struct{}, len(ids))
	for index, id := range ids {
		providerID, modelID, qualified := strings.Cut(id, "/")
		if id == "" || strings.TrimSpace(id) != id || !qualified || providerID == "" || modelID == "" {
			return fmt.Errorf("configured model %d %q is not a <provider>/<model> id", index, id)
		}

		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("configured model %q is listed twice", id)
		}

		seen[id] = struct{}{}
	}

	return nil
}
