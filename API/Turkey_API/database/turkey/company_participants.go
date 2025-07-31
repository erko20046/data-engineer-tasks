package turkey

import "time"

type CompanyParticipants struct {
	Id            int64 `gorm:"primaryKey;autoIncrement"`
	ParticipantId int64
	CompanyId     int64
	Relevance     time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
