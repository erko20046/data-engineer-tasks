package turkey

import "time"

type ContactsPhones struct {
	Id        int64 `gorm:"primaryKey;autoIncrement"`
	CompanyId int64
	Phone     string
	Relevance time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
