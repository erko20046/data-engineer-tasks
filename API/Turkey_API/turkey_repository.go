package turkey_repository

import (
	"api-extra-new/internal/model/database/turkey"
	"api-extra-new/internal/repository"
	"fmt"
	"gorm.io/gorm"
	"time"
)

// TurkeyRepository struct extends BaseRepository
type TurkeyRepository struct {
	repository.BaseRepository
}

func NewTurkeyRepository(baseRepo repository.BaseRepository) TurkeyRepository {
	return TurkeyRepository{BaseRepository: baseRepo}
}

func (r *TurkeyRepository) UpsertAddresses(tx *gorm.DB, addressName string, existingAddresses map[string]*turkey.Addresses) (*turkey.Addresses, map[string]*turkey.Addresses, error) {
	if existingAddress, exists := existingAddresses[addressName]; exists {
		return existingAddress, existingAddresses, nil
	}
	address := turkey.Addresses{Address: addressName, Relevance: time.Now()}

	if err := tx.Create(&address).Error; err != nil {
		return nil, existingAddresses, fmt.Errorf("error inserting Addresses: %v", err)
	}
	existingAddresses[addressName] = &address

	return &address, existingAddresses, nil
}

func (r *TurkeyRepository) UpsertFormerNames(tx *gorm.DB, formName string, existingForms map[string]*turkey.FormerNames) (*turkey.FormerNames, map[string]*turkey.FormerNames, error) {
	if existingForm, exists := existingForms[formName]; exists {
		return existingForm, existingForms, nil
	}
	form := turkey.FormerNames{FormerName: formName, Relevance: time.Now()}

	if err := tx.Create(&form).Error; err != nil {
		return nil, existingForms, fmt.Errorf("error inserting FormerName: %v", err)
	}
	existingForms[formName] = &form

	return &form, existingForms, nil
}

func (r *TurkeyRepository) UpsertIndustries(tx *gorm.DB, industryName string, existingIndustries map[string]*turkey.Industries) (*turkey.Industries, map[string]*turkey.Industries, error) {
	if existingIndustry, exists := existingIndustries[industryName]; exists {
		return existingIndustry, existingIndustries, nil
	}
	industry := turkey.Industries{Industry: industryName, Relevance: time.Now()}

	if err := tx.Create(&industry).Error; err != nil {
		return nil, existingIndustries, fmt.Errorf("error inserting Industrie: %v", err)
	}
	existingIndustries[industryName] = &industry

	return &industry, existingIndustries, nil
}

func (r *TurkeyRepository) UpsertMediaTypes(tx *gorm.DB, mediaTypeName string, existingMediaRegDocTypes map[string]*turkey.MediaRegDocTypes) (*turkey.MediaRegDocTypes, map[string]*turkey.MediaRegDocTypes, error) {
	if existingType, exists := existingMediaRegDocTypes[mediaTypeName]; exists {
		return existingType, existingMediaRegDocTypes, nil
	}
	mediaType := turkey.MediaRegDocTypes{Type: mediaTypeName, Relevance: time.Now()}

	if err := tx.Create(&mediaType).Error; err != nil {
		return nil, existingMediaRegDocTypes, fmt.Errorf("error inserting MediaType: %v", err)
	}
	existingMediaRegDocTypes[mediaTypeName] = &mediaType

	return &mediaType, existingMediaRegDocTypes, nil
}

func (r *TurkeyRepository) UpsertMediaTransactions(tx *gorm.DB, mediaTransactionName string, existingMediaTransactions map[string]*turkey.MediaTransactions) (*turkey.MediaTransactions, map[string]*turkey.MediaTransactions, error) {
	if existingTransaction, exists := existingMediaTransactions[mediaTransactionName]; exists {
		return existingTransaction, existingMediaTransactions, nil
	}
	transaction := turkey.MediaTransactions{Transaction: mediaTransactionName, Relevance: time.Now()}

	if err := tx.Create(&transaction).Error; err != nil {
		return nil, existingMediaTransactions, fmt.Errorf("error inserting MediaTransaction: %v", err)
	}
	existingMediaTransactions[mediaTransactionName] = &transaction

	return &transaction, existingMediaTransactions, nil
}

func (r *TurkeyRepository) UpsertOccupationGroups(tx *gorm.DB, occupationGroupName string, existingOccupationGroups map[string]*turkey.OccupationGroups) (*turkey.OccupationGroups, map[string]*turkey.OccupationGroups, error) {
	if existingOccupationGroup, exists := existingOccupationGroups[occupationGroupName]; exists {
		return existingOccupationGroup, existingOccupationGroups, nil
	}
	occupationGroup := turkey.OccupationGroups{OccupationGroup: occupationGroupName, Relevance: time.Now()}

	if err := tx.Create(&occupationGroup).Error; err != nil {
		return nil, existingOccupationGroups, fmt.Errorf("error inserting OccupationGroup: %v", err)
	}
	existingOccupationGroups[occupationGroupName] = &occupationGroup

	return &occupationGroup, existingOccupationGroups, nil
}

func (r *TurkeyRepository) UpsertParticipantNames(tx *gorm.DB, participantNames string, existingParticipantNames map[string]*turkey.ParticipantNames) (*turkey.ParticipantNames, map[string]*turkey.ParticipantNames, error) {
	if existingParticipantName, exists := existingParticipantNames[participantNames]; exists {
		return existingParticipantName, existingParticipantNames, nil
	}
	participantName := turkey.ParticipantNames{Name: participantNames, Relevance: time.Now()}

	if err := tx.Create(&participantName).Error; err != nil {
		return nil, existingParticipantNames, fmt.Errorf("error inserting ParticipantName: %v", err)
	}
	existingParticipantNames[participantNames] = &participantName

	return &participantName, existingParticipantNames, nil
}

func (r *TurkeyRepository) UpsertStatuses(tx *gorm.DB, StatusName string, existingStatuses map[string]*turkey.Statuses) (*turkey.Statuses, map[string]*turkey.Statuses, error) {
	if existingStatus, exists := existingStatuses[StatusName]; exists {
		return existingStatus, existingStatuses, nil
	}
	Status := turkey.Statuses{Status: StatusName, Relevance: time.Now()}

	if err := tx.Create(&Status).Error; err != nil {
		return nil, existingStatuses, fmt.Errorf("error inserting Status: %v", err)
	}
	existingStatuses[StatusName] = &Status

	return &Status, existingStatuses, nil
}

func (r *TurkeyRepository) UpsertTaxNumbers(tx *gorm.DB, TaxNumberName string, existingTaxNumbers map[string]*turkey.TaxNumbers) (*turkey.TaxNumbers, map[string]*turkey.TaxNumbers, error) {
	if existingTaxNumber, exists := existingTaxNumbers[TaxNumberName]; exists {
		return existingTaxNumber, existingTaxNumbers, nil
	}
	TaxNumber := turkey.TaxNumbers{TaxNumber: TaxNumberName, Relevance: time.Now()}

	if err := tx.Create(&TaxNumber).Error; err != nil {
		return nil, existingTaxNumbers, fmt.Errorf("error inserting TaxNumber: %v", err)
	}
	existingTaxNumbers[TaxNumberName] = &TaxNumber

	return &TaxNumber, existingTaxNumbers, nil
}

func (r *TurkeyRepository) UpsertParticipantStatus(tx *gorm.DB, ParticipantStatusName string, existingParticipantStatus map[string]*turkey.ParticipantTypes) (*turkey.ParticipantTypes, error) {
	if existingStatus, exists := existingParticipantStatus[ParticipantStatusName]; exists {
		return existingStatus, nil
	}
	ParticipantStatus := turkey.ParticipantTypes{Status: ParticipantStatusName, Relevance: time.Now()}

	if err := tx.Create(&ParticipantStatus).Error; err != nil {
		return nil, fmt.Errorf("error inserting ParticipantStatus: %v", err)
	}
	existingParticipantStatus[ParticipantStatusName] = &ParticipantStatus

	return &ParticipantStatus, nil
}

func (r *TurkeyRepository) UpsertParticipantPosition(tx *gorm.DB, participantPositionName string, existingParticipantPositions map[string]*turkey.ParticipantPositions) (*turkey.ParticipantPositions, map[string]*turkey.ParticipantPositions, error) {
	if existingPosition, exists := existingParticipantPositions[participantPositionName]; exists {
		return existingPosition, existingParticipantPositions, nil
	}
	participantPosition := turkey.ParticipantPositions{Position: participantPositionName, Relevance: time.Now()}

	if err := tx.Create(&participantPosition).Error; err != nil {
		return nil, existingParticipantPositions, fmt.Errorf("error inserting ParticipantPosition: %v", err)
	}
	existingParticipantPositions[participantPositionName] = &participantPosition

	return &participantPosition, existingParticipantPositions, nil
}

func (r *TurkeyRepository) UpsertParticipants(tx *gorm.DB, participant *turkey.Participants, existingParticipantsList map[string]*turkey.Participants) (*turkey.Participants, map[string]*turkey.Participants, error) {
	key := fmt.Sprintf("%d-%d-%d", participant.NameId, participant.PositionId, participant.TypeId)

	if existingParticipant, exists := existingParticipantsList[key]; exists {
		existingParticipant.Capital = participant.Capital
		existingParticipant.Relevance = participant.Relevance

		if err := tx.Save(existingParticipant).Error; err != nil {
			return nil, existingParticipantsList, fmt.Errorf("error updating existing Participant: %v", err)
		}
		return existingParticipant, existingParticipantsList, nil
	}

	if err := tx.Create(&participant).Error; err != nil {
		return nil, existingParticipantsList, fmt.Errorf("error inserting Participant: %v", err)
	}
	existingParticipantsList[key] = participant

	return participant, existingParticipantsList, nil
}

func (r *TurkeyRepository) UpsertCompany(tx *gorm.DB, company *turkey.Companies, existingCompaniesList map[string]*turkey.Companies) (*turkey.Companies, map[string]*turkey.Companies, error) {
	if existingCompany, exists := existingCompaniesList[company.Uin]; exists {
		existingCompany.ChamberRegNo = company.ChamberRegNo
		existingCompany.CentralRegSystemNo = company.CentralRegSystemNo
		existingCompany.Name = company.Name
		existingCompany.StatusId = company.StatusId
		existingCompany.RegDate = company.RegDate
		existingCompany.AddressId = company.AddressId
		existingCompany.MainContractRegDate = company.MainContractRegDate
		existingCompany.TaxNumberId = company.TaxNumberId
		existingCompany.Capital = company.Capital
		existingCompany.OccupationGroupId = company.OccupationGroupId
		existingCompany.Relevance = company.Relevance

		if err := tx.Save(existingCompany).Error; err != nil {
			return nil, existingCompaniesList, fmt.Errorf("error updating existing Company: %v", err)
		}
		return existingCompany, existingCompaniesList, nil
	}

	if err := tx.Create(&company).Error; err != nil {
		return nil, existingCompaniesList, fmt.Errorf("error inserting Company: %v", err)
	}
	existingCompaniesList[company.Uin] = company

	return company, existingCompaniesList, nil
}

func (r *TurkeyRepository) UpsertCompanyFormerNames(tx *gorm.DB, companyFormerName *turkey.CompanyFormerNames, existingCompanyFormerNamesList map[string]*turkey.CompanyFormerNames) (map[string]*turkey.CompanyFormerNames, error) {
	key := fmt.Sprintf("%d-%d", companyFormerName.FormerNameId, companyFormerName.CompanyId)

	if _, exists := existingCompanyFormerNamesList[key]; exists {
		return existingCompanyFormerNamesList, nil
	}

	if err := tx.Create(&companyFormerName).Error; err != nil {
		return existingCompanyFormerNamesList, fmt.Errorf("error inserting CompanyFormerName: %v", err)
	}
	existingCompanyFormerNamesList[key] = companyFormerName

	return existingCompanyFormerNamesList, nil
}

func (r *TurkeyRepository) UpsertCompanyIndustries(tx *gorm.DB, companyIndustries *turkey.CompanyIndustries, existingCompanyIndustriesList map[string]*turkey.CompanyIndustries) (map[string]*turkey.CompanyIndustries, error) {
	key := fmt.Sprintf("%d-%d", companyIndustries.IndustryId, companyIndustries.CompanyId)

	if _, exists := existingCompanyIndustriesList[key]; exists {
		return existingCompanyIndustriesList, nil
	}

	if err := tx.Create(&companyIndustries).Error; err != nil {
		return existingCompanyIndustriesList, fmt.Errorf("error inserting CompanyIndustry: %v", err)
	}
	existingCompanyIndustriesList[key] = companyIndustries

	return existingCompanyIndustriesList, nil
}

func (r *TurkeyRepository) UpsertCompanyMedias(tx *gorm.DB, companyMedias *turkey.CompanyMedias, existingCompanyMediasList map[string]*turkey.CompanyMedias) (map[string]*turkey.CompanyMedias, error) {
	key := fmt.Sprintf("%d-%d-%d-%d-%s", companyMedias.DocumentNumber, companyMedias.TransactionId, companyMedias.RegDocTypeId, companyMedias.CompanyId, companyMedias.RegHistory)

	if _, exists := existingCompanyMediasList[key]; exists {
		return existingCompanyMediasList, nil
	}

	if err := tx.Create(&companyMedias).Error; err != nil {
		return existingCompanyMediasList, fmt.Errorf("error inserting CompanyMedia: %v", err)
	}
	existingCompanyMediasList[key] = companyMedias

	return existingCompanyMediasList, nil
}

func (r *TurkeyRepository) UpsertCompanyParticipants(tx *gorm.DB, companyParticipant *turkey.CompanyParticipants, existingCompanyParticipantsList map[string]*turkey.CompanyParticipants) (map[string]*turkey.CompanyParticipants, error) {
	key := fmt.Sprintf("%d-%d", companyParticipant.ParticipantId, companyParticipant.CompanyId)

	if _, exists := existingCompanyParticipantsList[key]; exists {
		return existingCompanyParticipantsList, nil
	}

	if err := tx.Create(&companyParticipant).Error; err != nil {
		return existingCompanyParticipantsList, fmt.Errorf("error inserting CompanyParticipant: %v", err)
	}
	existingCompanyParticipantsList[key] = companyParticipant

	return existingCompanyParticipantsList, nil
}

func (r *TurkeyRepository) UpsertContactsFaxes(tx *gorm.DB, contactsFaxe *turkey.ContactsFaxes, existingContactsFaxesList map[string]*turkey.ContactsFaxes) (map[string]*turkey.ContactsFaxes, error) {
	key := fmt.Sprintf("%d-%s", contactsFaxe.CompanyId, contactsFaxe.Fax)

	if _, exists := existingContactsFaxesList[key]; exists {
		return existingContactsFaxesList, nil
	}

	if err := tx.Create(&contactsFaxe).Error; err != nil {
		return existingContactsFaxesList, fmt.Errorf("error inserting ContactsFaxe: %v", err)
	}
	existingContactsFaxesList[key] = contactsFaxe

	return existingContactsFaxesList, nil
}

func (r *TurkeyRepository) UpsertContactsPhones(tx *gorm.DB, contactsPhone *turkey.ContactsPhones, existingContactsPhonesList map[string]*turkey.ContactsPhones) (map[string]*turkey.ContactsPhones, error) {
	key := fmt.Sprintf("%d-%s", contactsPhone.CompanyId, contactsPhone.Phone)

	if _, exists := existingContactsPhonesList[key]; exists {
		return existingContactsPhonesList, nil
	}

	if err := tx.Create(&contactsPhone).Error; err != nil {
		return existingContactsPhonesList, fmt.Errorf("error inserting ContactsPhone: %v", err)
	}
	existingContactsPhonesList[key] = contactsPhone

	return existingContactsPhonesList, nil
}

func (r *TurkeyRepository) UpsertContactsWebsites(tx *gorm.DB, contactsWebsite *turkey.ContactsWebsites, existingContactsWebsitesList map[string]*turkey.ContactsWebsites) (map[string]*turkey.ContactsWebsites, error) {
	key := fmt.Sprintf("%d-%s", contactsWebsite.CompanyId, contactsWebsite.Website)

	if _, exists := existingContactsWebsitesList[key]; exists {
		return existingContactsWebsitesList, nil
	}

	if err := tx.Create(&contactsWebsite).Error; err != nil {
		return existingContactsWebsitesList, fmt.Errorf("error inserting ContactsWebsite: %v", err)
	}
	existingContactsWebsitesList[key] = contactsWebsite

	return existingContactsWebsitesList, nil
}

func (r *TurkeyRepository) UpsertFormerOfficialsDates(tx *gorm.DB, formerOfficialsDate *turkey.FormerOfficialsDates, existingFormerOfficialsDateList map[string]*turkey.FormerOfficialsDates) (map[string]*turkey.FormerOfficialsDates, error) {
	key := fmt.Sprintf("%d", formerOfficialsDate.ParticipantId)

	if existingFormerOfficialsDate, exists := existingFormerOfficialsDateList[key]; exists {
		existingFormerOfficialsDate.EndDate = formerOfficialsDate.EndDate
		existingFormerOfficialsDate.Relevance = formerOfficialsDate.Relevance

		if err := tx.Save(existingFormerOfficialsDate).Error; err != nil {
			return existingFormerOfficialsDateList, fmt.Errorf("error updating existing FormerOfficialDate: %v", err)
		}
		return existingFormerOfficialsDateList, nil
	}

	if err := tx.Create(&formerOfficialsDate).Error; err != nil {
		return existingFormerOfficialsDateList, fmt.Errorf("error inserting Former fficialDate: %v", err)
	}
	existingFormerOfficialsDateList[key] = formerOfficialsDate

	return existingFormerOfficialsDateList, nil
}
