package turkey

import "time"

type MediaTransactions struct {
	Id          int64 `gorm:"primaryKey;autoIncrement"`
	Transaction string
	Relevance   time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
