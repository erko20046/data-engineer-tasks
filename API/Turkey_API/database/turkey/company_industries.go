package turkey

import "time"

type CompanyIndustries struct {
	Id         int64 `gorm:"primaryKey;autoIncrement"`
	IndustryId int64
	CompanyId  int64
	Relevance  time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
