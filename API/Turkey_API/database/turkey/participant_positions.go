package turkey

import "time"

type ParticipantPositions struct {
	Id        int64 `gorm:"primaryKey;autoIncrement"`
	Position  string
	Relevance time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
