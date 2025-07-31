package turkey

import "time"

type ContactsWebsites struct {
	Id        int64 `gorm:"primaryKey;autoIncrement"`
	CompanyId int64
	Website   string
	Relevance time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
