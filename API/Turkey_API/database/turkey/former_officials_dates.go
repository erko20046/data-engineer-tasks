package turkey

import "time"

type FormerOfficialsDates struct {
	Id            int64 `gorm:"primaryKey;autoIncrement"`
	ParticipantId int64
	EndDate       *time.Time
	Relevance     time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
