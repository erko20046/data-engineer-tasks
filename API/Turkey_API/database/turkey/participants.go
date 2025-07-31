package turkey

import "time"

type Participants struct {
	Id         int64 `gorm:"primaryKey;autoIncrement"`
	NameId     int64
	PositionId int64
	Capital    *float64
	TypeId     int8
	Relevance  time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
