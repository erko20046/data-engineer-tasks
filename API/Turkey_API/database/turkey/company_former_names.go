package turkey

import "time"

type CompanyFormerNames struct {
	Id           int64 `gorm:"primaryKey;autoIncrement"`
	FormerNameId int64
	CompanyId    int64
	Relevance    time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
