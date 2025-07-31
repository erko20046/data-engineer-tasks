package turkey

import "time"

type Addresses struct {
	Id        int64 `gorm:"primaryKey;autoIncrement"`
	Address   string
	Relevance time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
