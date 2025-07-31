package turkey

import "time"

type CompanyMedias struct {
	Id             int64 `gorm:"primaryKey;autoIncrement"`
	DocumentNumber int32
	TransactionId  int64
	RegDocTypeId   int64
	CompanyId      int64
	RegHistory     *time.Time
	Relevance      time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
