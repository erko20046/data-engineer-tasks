package turkey

import "time"

type ParticipantTypes struct {
	Id        int64 `gorm:"primaryKey;autoIncrement"`
	Status    string
	Relevance time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
