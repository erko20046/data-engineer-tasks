package turkey

import "time"

type ContactsFaxes struct {
	Id        int64 `gorm:"primaryKey;autoIncrement"`
	CompanyId int64
	Fax       string
	Relevance time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
