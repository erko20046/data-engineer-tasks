package turkey

import "time"

type OccupationGroups struct {
	Id              int64 `gorm:"primaryKey;autoIncrement"`
	OccupationGroup string
	Relevance       time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
