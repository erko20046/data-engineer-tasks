package turkey

import "time"

type FormerNames struct {
	Id         int64 `gorm:"primaryKey;autoIncrement"`
	FormerName string
	Relevance  time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
