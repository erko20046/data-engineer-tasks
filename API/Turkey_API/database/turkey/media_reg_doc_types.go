package turkey

import "time"

type MediaRegDocTypes struct {
	Id        int64 `gorm:"primaryKey;autoIncrement"`
	Type      string
	Relevance time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
