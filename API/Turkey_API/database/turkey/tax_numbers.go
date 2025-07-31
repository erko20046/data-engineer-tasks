package turkey

import "time"

type TaxNumbers struct {
	Id        int64 `gorm:"primaryKey;autoIncrement"`
	TaxNumber string
	Relevance time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
