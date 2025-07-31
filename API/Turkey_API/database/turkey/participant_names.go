package turkey

import "time"

type ParticipantNames struct {
	Id        int64 `gorm:"primaryKey;autoIncrement"`
	Name      string
	Relevance time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
