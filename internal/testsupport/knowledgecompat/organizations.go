package knowledgecompat

import (
	"testing"

	"gorm.io/gorm"
)

// BindIndexes establishes explicit native ownership for the named fixture
// indexes. It never invents indexes or changes an existing owner's authority.
// Production installs the full GoCommon-backed ownership migration.
func BindIndexes(t *testing.T, database *gorm.DB, organizationID int64, names ...string) {
	t.Helper()
	if database == nil || organizationID <= 0 || len(names) == 0 {
		t.Fatal("native knowledge index ownership requires a database, organization, and explicit indexes")
	}
	if err := database.WithContext(t.Context()).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`CREATE TABLE IF NOT EXISTS os_index_organizations (index_id TEXT PRIMARY KEY REFERENCES indexes(index_id), organization_id BIGINT NOT NULL CHECK (organization_id > 0))`).Error; err != nil {
			return err
		}
		if err := tx.Exec(`INSERT INTO os_index_organizations(index_id,organization_id) SELECT index_id,? FROM indexes WHERE name IN ? ON CONFLICT(index_id) DO NOTHING`, organizationID, names).Error; err != nil {
			return err
		}
		var count int64
		if err := tx.Table("indexes AS target").Joins("JOIN os_index_organizations AS ownership ON ownership.index_id=target.index_id").Where("ownership.organization_id=? AND target.name IN ?", organizationID, names).Count(&count).Error; err != nil {
			return err
		}
		if count != int64(len(names)) {
			t.Fatalf("explicit native ownership matched %d indexes, want %d", count, len(names))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
