package turkey

import "time"

type Industries struct {
	Id        int64 `gorm:"primaryKey;autoIncrement"`
	Industry  string
	Relevance time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
