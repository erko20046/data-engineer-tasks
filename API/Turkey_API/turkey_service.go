package service

import (
	"api-extra-new/internal/model/database/abstract_db"
	turkey2 "api-extra-new/internal/model/database/turkey"
	pkg3 "api-extra-new/internal/repository"
	pkg2 "api-extra-new/internal/repository/turkey_repository"
	"api-extra-new/pkg"
	"api-extra-new/protos/generated/turkey"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"golang.org/x/net/context"
	"strconv"
	"strings"
	"time"
)

func TurkeyService(nameOrUin string, client turkey.TurkeyCompaniesScraperClient, repository pkg2.TurkeyRepository, logger pkg.Logger, es *elasticsearch.Client, index string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nameOrUin = strings.TrimSpace(nameOrUin)

	req := &turkey.LoadDataRequest{Name: nameOrUin}
	reply, exc := client.LoadData(ctx, req)

	if exc != nil {
		logger.Error("Error processing data in microservice", exc.Error())
		return exc
	}

	tx := repository.Database.DB.Begin()

	var err error

	// tr_addresses
	var AddressesList []turkey2.Addresses
	tx.Find(&AddressesList)
	existingAddresses := make(map[string]*turkey2.Addresses)
	for _, item := range AddressesList {
		existingAddresses[item.Address] = &item
	}

	// tr_former_names
	var FormerNamesList []turkey2.FormerNames
	tx.Find(&FormerNamesList)
	existingFormerNames := make(map[string]*turkey2.FormerNames)
	for _, item := range FormerNamesList {
		existingFormerNames[item.FormerName] = &item
	}

	// tr_industries
	var IndustriesList []turkey2.Industries
	tx.Find(&IndustriesList)
	existingIndustries := make(map[string]*turkey2.Industries)
	for _, item := range IndustriesList {
		existingIndustries[item.Industry] = &item
	}

	// tr_media_reg_doc_types
	var MediaRegDocTypesList []turkey2.MediaRegDocTypes
	tx.Find(&MediaRegDocTypesList)
	existingMediaRegDocTypes := make(map[string]*turkey2.MediaRegDocTypes)
	for _, item := range MediaRegDocTypesList {
		existingMediaRegDocTypes[item.Type] = &item
	}

	// tr_media_transactions
	var MediaTransactionsList []turkey2.MediaTransactions
	tx.Find(&MediaTransactionsList)
	existingMediaTransactions := make(map[string]*turkey2.MediaTransactions)
	for _, item := range MediaTransactionsList {
		existingMediaTransactions[item.Transaction] = &item
	}

	// tr_occupation_groups
	var OccupationGroupsList []turkey2.OccupationGroups
	tx.Find(&OccupationGroupsList)
	existingOccupationGroups := make(map[string]*turkey2.OccupationGroups)
	for _, item := range OccupationGroupsList {
		existingOccupationGroups[item.OccupationGroup] = &item
	}

	// tr_participant_names
	var ParticipantNamesList []turkey2.ParticipantNames
	tx.Find(&ParticipantNamesList)
	existingParticipantNames := make(map[string]*turkey2.ParticipantNames)
	for _, item := range ParticipantNamesList {
		existingParticipantNames[item.Name] = &item
	}

	// tr_participant_positions
	var ParticipantPositionsList []turkey2.ParticipantPositions
	tx.Find(&ParticipantPositionsList)
	existingParticipantPositions := make(map[string]*turkey2.ParticipantPositions)
	for _, item := range ParticipantPositionsList {
		existingParticipantPositions[item.Position] = &item
	}

	// tr_participant_types
	var ParticipantTypesList []turkey2.ParticipantTypes
	tx.Find(&ParticipantTypesList)
	existingParticipantTypes := make(map[string]*turkey2.ParticipantTypes)
	for _, item := range ParticipantTypesList {
		existingParticipantTypes[item.Status] = &item
	}

	// tr_statuses
	var StatusesList []turkey2.Statuses
	tx.Find(&StatusesList)
	existingStatuses := make(map[string]*turkey2.Statuses)
	for _, item := range StatusesList {
		existingStatuses[item.Status] = &item
	}

	// tr_tax_numbers
	var TaxNumbersList []turkey2.TaxNumbers
	tx.Find(&TaxNumbersList)
	existingTaxNumbers := make(map[string]*turkey2.TaxNumbers)
	for _, item := range TaxNumbersList {
		existingTaxNumbers[item.TaxNumber] = &item
	}

	// tr_companies
	var CompaniesList []turkey2.Companies
	tx.Find(&CompaniesList)
	existingCompaniesList := make(map[string]*turkey2.Companies)
	for _, item := range CompaniesList {
		existingCompaniesList[item.Uin] = &item
	}

	// participants
	var ParticipantsList []turkey2.Participants
	tx.Find(&ParticipantsList)
	existingParticipantsList := make(map[string]*turkey2.Participants)
	for _, item := range ParticipantsList {
		key := fmt.Sprintf("%d-%d-%d", item.NameId, item.PositionId, item.TypeId)
		existingParticipantsList[key] = &item
	}

	// Company Former Names
	var companyFormerNamesList []turkey2.CompanyFormerNames
	tx.Find(&companyFormerNamesList)
	existingCompanyFormerNamesList := make(map[string]*turkey2.CompanyFormerNames)
	for _, item := range companyFormerNamesList {
		key := fmt.Sprintf("%d-%d", item.FormerNameId, item.CompanyId)
		existingCompanyFormerNamesList[key] = &item
	}

	// Company Industries
	var companyIndustriesList []turkey2.CompanyIndustries
	tx.Find(&companyIndustriesList)
	existingCompanyIndustriesList := make(map[string]*turkey2.CompanyIndustries)
	for _, item := range companyIndustriesList {
		key := fmt.Sprintf("%d-%d", item.IndustryId, item.CompanyId)
		existingCompanyIndustriesList[key] = &item
	}

	// Company Media
	var companyMediasList []turkey2.CompanyMedias
	tx.Find(&companyMediasList)
	existingCompanyMediasList := make(map[string]*turkey2.CompanyMedias)
	for _, item := range companyMediasList {
		key := fmt.Sprintf("%d-%d-%d-%d-%s", item.DocumentNumber, item.TransactionId, item.RegDocTypeId, item.CompanyId, item.RegHistory)
		existingCompanyMediasList[key] = &item
	}

	// Company Participants
	var companyParticipantsList []turkey2.CompanyParticipants
	tx.Find(&companyParticipantsList)
	existingCompanyParticipantsList := make(map[string]*turkey2.CompanyParticipants)
	for _, item := range companyParticipantsList {
		key := fmt.Sprintf("%d-%d", item.ParticipantId, item.CompanyId)
		existingCompanyParticipantsList[key] = &item
	}

	// Former Officials Dates
	var formerOfficialsDatesList []turkey2.FormerOfficialsDates
	tx.Find(&formerOfficialsDatesList)
	existingFormerOfficialsDatesList := make(map[string]*turkey2.FormerOfficialsDates)
	for _, item := range formerOfficialsDatesList {
		key := fmt.Sprintf("%d", item.ParticipantId)
		existingFormerOfficialsDatesList[key] = &item
	}

	// Contacts Faxes
	var contactsFaxesList []turkey2.ContactsFaxes
	tx.Find(&contactsFaxesList)
	existingContactsFaxesList := make(map[string]*turkey2.ContactsFaxes)
	for _, item := range contactsFaxesList {
		key := fmt.Sprintf("%d-%s", item.CompanyId, item.Fax)
		existingContactsFaxesList[key] = &item
	}

	// Contacts Phones
	var contactsPhonesList []turkey2.ContactsPhones
	tx.Find(&contactsPhonesList)
	existingContactsPhonesList := make(map[string]*turkey2.ContactsPhones)
	for _, item := range contactsPhonesList {
		key := fmt.Sprintf("%d-%s", item.CompanyId, item.Phone)
		existingContactsPhonesList[key] = &item
	}

	// Contacts Website
	var contactsWebsitesList []turkey2.ContactsWebsites
	tx.Find(&contactsWebsitesList)
	existingContactsWebsitesList := make(map[string]*turkey2.ContactsWebsites)
	for _, item := range contactsWebsitesList {
		key := fmt.Sprintf("%d-%s", item.CompanyId, item.Website)
		existingContactsWebsitesList[key] = &item
	}

	var addressId *int64
	var occupationGroupId *int64
	var statusId *int8
	var taxNumberId int64
	var companyId int64

	for i := 0; i < len(reply.Data.Units); i++ {
		if reply.Data.Units[i] != nil {
			if reply.Data.Units[i].Address != nil {
				address, updatedAddress, err := repository.UpsertAddresses(tx, reply.Data.Units[i].Address.Value, existingAddresses)

				if logAndReturnIfError(err, "Error upserting Addresses") != nil {
					tx.Rollback()
					return err
				}
				existingAddresses = updatedAddress
				addressId = &address.Id
			}

			if reply.Data.Units[i].OccupationalGroup != nil {
				occupationGroup, updatedOccupationGroups, err := repository.UpsertOccupationGroups(tx, reply.Data.Units[i].OccupationalGroup.Value, existingOccupationGroups)

				if logAndReturnIfError(err, "Error upserting OccupationalGroup") != nil {
					tx.Rollback()
					return err
				}
				existingOccupationGroups = updatedOccupationGroups
				occupationGroupId = &occupationGroup.Id
			}

			if reply.Data.Units[i].Status != nil {
				Status, updatedStatus, err := repository.UpsertStatuses(tx, reply.Data.Units[i].Status.Value, existingStatuses)

				if logAndReturnIfError(err, "Error upserting Statuses") != nil {
					tx.Rollback()
					return err
				}
				existingStatuses = updatedStatus

				tmp := int8(Status.Id)
				statusId = &tmp

			}

			if reply.Data.Units[i].TaxNumber != nil {
				TaxNumber, updatedTaxNumbers, err := repository.UpsertTaxNumbers(tx, reply.Data.Units[i].Status.Value, existingTaxNumbers)

				if logAndReturnIfError(err, "Error upserting TaxNumbers") != nil {
					tx.Rollback()
					return err
				}
				existingTaxNumbers = updatedTaxNumbers
				taxNumberId = TaxNumber.Id
			}

			var ChamberRegNo *string
			if reply.Data.Units[i].ChamberRegNo != nil {
				ChamberRegNo = &reply.Data.Units[i].ChamberRegNo.Value
			}

			var CentralRegSystemNo *string
			if reply.Data.Units[i].CentralRegSystemNo != nil {
				CentralRegSystemNo = &reply.Data.Units[i].CentralRegSystemNo.Value
			}

			var Name string
			if reply.Data.Units[i].Name != nil {
				Name = reply.Data.Units[i].Name.Value
			}

			var RegDate *time.Time
			if reply.Data.Units[i].RegDate != nil {
				t := reply.Data.Units[i].RegDate.AsTime()
				RegDate = &t
			}

			var MainContractRegDate *time.Time
			if reply.Data.Units[i].MainContractRegDate != nil {
				t := reply.Data.Units[i].MainContractRegDate.AsTime()
				MainContractRegDate = &t
			}

			var Capital *float64
			if reply.Data.Units[i].Capital != nil {
				valStr := reply.Data.Units[i].Capital.Value
				if val, err := strconv.ParseFloat(valStr, 64); err == nil {
					Capital = &val
				}
			}

			var Uin string
			if reply.Data.Units[i].Uin != nil {
				Uin = reply.Data.Units[i].Uin.Value
			}

			companies := turkey2.Companies{
				Uin:                 Uin,
				ChamberRegNo:        ChamberRegNo,
				CentralRegSystemNo:  CentralRegSystemNo,
				Name:                Name,
				StatusId:            statusId,
				RegDate:             RegDate,
				AddressId:           addressId,
				MainContractRegDate: MainContractRegDate,
				TaxNumberId:         taxNumberId,
				Capital:             Capital,
				OccupationGroupId:   occupationGroupId,
				Relevance:           time.Now(),
			}

			company, updatedCompaniesList, err := repository.UpsertCompany(tx, &companies, existingCompaniesList)
			if logAndReturnIfError(err, "Error upserting Companies") != nil {
				tx.Rollback()
				return err
			}
			existingCompaniesList = updatedCompaniesList
			companyId = company.Id

			// tr_contacts_faxes
			if reply.Data.Units[i].Contacts != nil && reply.Data.Units[i].Contacts.Fax != nil {
				contactsFaxes := turkey2.ContactsFaxes{
					CompanyId: companyId,
					Fax:       reply.Data.Units[i].Contacts.Fax.Value,
				}

				updatedContactsFaxesList, err := repository.UpsertContactsFaxes(tx, &contactsFaxes, existingContactsFaxesList)
				if logAndReturnIfError(err, "Error updating ContactsFaxes") != nil {
					tx.Rollback()
					return err
				}
				existingContactsFaxesList = updatedContactsFaxesList
			}

			// tr_contacts_phones
			if reply.Data.Units[i].Contacts != nil && reply.Data.Units[i].Contacts.Phone != nil {
				contactsPhone := turkey2.ContactsPhones{
					CompanyId: companyId,
					Phone:     reply.Data.Units[i].Contacts.Phone.Value,
				}

				updatedContactsPhonesList, err := repository.UpsertContactsPhones(tx, &contactsPhone, existingContactsPhonesList)
				if logAndReturnIfError(err, "Error updating ContactsPhones") != nil {
					tx.Rollback()
					return err
				}
				existingContactsPhonesList = updatedContactsPhonesList

			}

			// tr_contacts_websites
			if reply.Data.Units[i].Contacts != nil && reply.Data.Units[i].Contacts.Website != nil {
				contactsWebsite := turkey2.ContactsWebsites{
					CompanyId: companyId,
					Website:   reply.Data.Units[i].Contacts.Website.Value,
				}

				updatedContactsWebsitesList, err := repository.UpsertContactsWebsites(tx, &contactsWebsite, existingContactsWebsitesList)
				if logAndReturnIfError(err, "Error updating Contact ContactsWebsites") != nil {
					tx.Rollback()
					return err
				}
				existingContactsWebsitesList = updatedContactsWebsitesList

			}

			// tr_former_names & tr_company_former_names
			if reply.Data.Units[i].FormerNames != nil {
				for _, former := range reply.Data.Units[i].FormerNames {
					if former.Name == nil {
						continue
					}

					formerName, updatedFormerName, err := repository.UpsertFormerNames(tx, former.Name.Value, existingFormerNames)

					if logAndReturnIfError(err, "Error upserting FormerNames") != nil {
						tx.Rollback()
						return err
					}
					existingFormerNames = updatedFormerName

					companyFormerNames := turkey2.CompanyFormerNames{
						FormerNameId: formerName.Id,
						CompanyId:    companyId,
					}

					updatedCompanyFormerNamesList, err := repository.UpsertCompanyFormerNames(tx, &companyFormerNames, existingCompanyFormerNamesList)
					if logAndReturnIfError(err, "Error updating CompanyFormerNames") != nil {
						tx.Rollback()
						return err
					}
					existingCompanyFormerNamesList = updatedCompanyFormerNamesList

				}
			}
			// tr_industries && tr_company_industries
			if reply.Data.Units[i].Industries != nil {
				for _, former := range reply.Data.Units[i].Industries {
					if former.Branch == nil {
						continue
					}

					industryName, updatedIndustries, err := repository.UpsertIndustries(tx, former.Branch.Value, existingIndustries)

					if logAndReturnIfError(err, "Error upserting Industries") != nil {
						tx.Rollback()
						return err
					}
					existingIndustries = updatedIndustries

					companyIndustries := turkey2.CompanyIndustries{
						IndustryId: industryName.Id,
						CompanyId:  companyId,
					}

					updatedCompanyIndustriesList, err := repository.UpsertCompanyIndustries(tx, &companyIndustries, existingCompanyIndustriesList)
					if logAndReturnIfError(err, "Error updating CompanyIndustries") != nil {
						tx.Rollback()
						return err
					}
					existingCompanyIndustriesList = updatedCompanyIndustriesList
				}
			}

			if reply.Data.Units[i].Media != nil {
				for _, media := range reply.Data.Units[i].Media {
					if media.RegDocType == nil || media.Transaction == nil || media.DocNumber == nil || media.RegHistory == nil {
						continue
					}

					// RegDocType
					mediaType, updatedMediaType, err := repository.UpsertMediaTypes(tx, media.RegDocType.Value, existingMediaRegDocTypes)
					if logAndReturnIfError(err, "Error upserting Media.RegDocType") != nil {
						tx.Rollback()
						return err
					}
					existingMediaRegDocTypes = updatedMediaType

					// Transaction
					transaction, updatedTransaction, err := repository.UpsertMediaTransactions(tx, media.Transaction.Value, existingMediaTransactions)
					if logAndReturnIfError(err, "Error upserting Media.Transaction") != nil {
						tx.Rollback()
						return err
					}
					existingMediaTransactions = updatedTransaction

					docNumberInt, err := strconv.ParseInt(media.DocNumber.Value, 10, 32)
					if err != nil {
						return err
					}
					docNumber := int32(docNumberInt)

					var regHistory *time.Time
					if media.RegHistory != nil {
						t := media.RegHistory.AsTime()
						regHistory = &t
					}

					companyMedias := turkey2.CompanyMedias{
						DocumentNumber: docNumber,
						TransactionId:  transaction.Id,
						RegDocTypeId:   mediaType.Id,
						CompanyId:      companyId,
						RegHistory:     regHistory,
					}

					updatedCompanyMediasList, err := repository.UpsertCompanyMedias(tx, &companyMedias, existingCompanyMediasList)
					if logAndReturnIfError(err, "Error updating CompanyMedias") != nil {
						tx.Rollback()
						return err
					}
					existingCompanyMediasList = updatedCompanyMediasList

				}
			}
			// insert or update Participant
			// statusesToUpsert — список всех статусов
			statusesToUpsert := []string{
				"ПАРТНЁР",
				"БЫВШИЙ ПАРТНЁР",
				"ДИРЕКТОР",
				"БЫВШИЙ ДИРЕКТОР",
				"УПОЛНОМОЧЕННОЕ ЛИЦО",
				"БЫВШЕЕ УПОЛНОМОЧЕННОЕ ЛИЦО",
			}

			statusIDs := make(map[string]int)

			for _, status := range statusesToUpsert {
				participantStatus, err := repository.UpsertParticipantStatus(tx, status, existingParticipantTypes)
				if logAndReturnIfError(err, "Error upserting ParticipantStatus") != nil {
					tx.Rollback()
					return err
				}
				statusIDs[status] = int(participantStatus.Id)
			}

			partnerStatusId := statusIDs["ПАРТНЁР"]
			formerPartnerStatusId := statusIDs["БЫВШИЙ ПАРТНЁР"]
			directorBoardId := statusIDs["ДИРЕКТОР"]
			formerDirectorBoardId := statusIDs["БЫВШИЙ ДИРЕКТОР"]
			officialStatusId := statusIDs["УПОЛНОМОЧЕННОЕ ЛИЦО"]
			formerOfficialStatusId := statusIDs["БЫВШЕЕ УПОЛНОМОЧЕННОЕ ЛИЦО"]

			// insert or update participants && Company participants
			// partners
			if reply.Data.Units[i].Partners != nil {
				for _, former := range reply.Data.Units[i].Partners {
					if former.Position == nil || former.Name == nil || former.Capital == nil {
						continue
					}

					partnerPosition, updatedPartnerPosition, err := repository.UpsertParticipantPosition(tx, former.Position.Value, existingParticipantPositions)

					if logAndReturnIfError(err, "Error upserting ParticipantPosition") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantPositions = updatedPartnerPosition

					partnerName, updatedPartnerName, err := repository.UpsertParticipantNames(tx, former.Name.Value, existingParticipantNames)

					if logAndReturnIfError(err, "Error upserting ParticipantNames") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantNames = updatedPartnerName

					var capital *float64
					if former.Capital != nil {
						valStr := former.Capital.Value
						if val, err := strconv.ParseFloat(valStr, 64); err == nil {
							capital = &val
						}
					}

					participants := turkey2.Participants{
						NameId:     partnerName.Id,
						PositionId: partnerPosition.Id,
						Capital:    capital,
						TypeId:     int8(partnerStatusId),
					}

					participant, updatedParticipantsList, err := repository.UpsertParticipants(tx, &participants, existingParticipantsList)
					if logAndReturnIfError(err, "Error upserting Participants") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantsList = updatedParticipantsList

					// Company Participants part
					companyParticipants := turkey2.CompanyParticipants{
						ParticipantId: participant.Id,
						CompanyId:     companyId,
					}

					updatedCompanyParticipantsList, err := repository.UpsertCompanyParticipants(tx, &companyParticipants, existingCompanyParticipantsList)
					if logAndReturnIfError(err, "Error upserting CompanyParticipants") != nil {
						tx.Rollback()
						return err
					}
					existingCompanyParticipantsList = updatedCompanyParticipantsList
				}
			}
			// Former Partners
			if reply.Data.Units[i].FormerPartners != nil {
				for _, former := range reply.Data.Units[i].FormerPartners {
					if former.Position == nil || former.Name == nil || former.Capital == nil {
						continue
					}

					// former partner
					formerPartnerPosition, updatedFormerPartnerPosition, err := repository.UpsertParticipantPosition(tx, former.Position.Value, existingParticipantPositions)

					if logAndReturnIfError(err, "Error upserting participant ParticipantPosition") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantPositions = updatedFormerPartnerPosition

					formerPartnerName, updatedFormerPartnerName, err := repository.UpsertParticipantNames(tx, former.Name.Value, existingParticipantNames)

					if logAndReturnIfError(err, "Error upserting ParticipantNames") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantNames = updatedFormerPartnerName

					var capital *float64
					if former.Capital != nil {
						valStr := former.Capital.Value
						if val, err := strconv.ParseFloat(valStr, 64); err == nil {
							capital = &val
						}
					}

					participants := turkey2.Participants{
						NameId:     formerPartnerName.Id,
						PositionId: formerPartnerPosition.Id,
						Capital:    capital,
						TypeId:     int8(formerPartnerStatusId),
					}

					participant, updatedParticipantsList, err := repository.UpsertParticipants(tx, &participants, existingParticipantsList)
					if logAndReturnIfError(err, "Error upserting Participants") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantsList = updatedParticipantsList

					// Company Participants part
					companyParticipants := turkey2.CompanyParticipants{
						ParticipantId: participant.Id,
						CompanyId:     companyId,
					}

					updatedCompanyParticipantsList, err := repository.UpsertCompanyParticipants(tx, &companyParticipants, existingCompanyParticipantsList)
					if logAndReturnIfError(err, "Error updating CompanyParticipants") != nil {
						tx.Rollback()
						return err
					}
					existingCompanyParticipantsList = updatedCompanyParticipantsList
				}
			}
			// director
			if reply.Data.Units[i].DirectorBoard != nil {
				for _, former := range reply.Data.Units[i].DirectorBoard {
					if former.Position == nil || former.Name == nil || former.Capital == nil {
						continue
					}

					directorBoardPosition, updatedDirectorBoardPosition, err := repository.UpsertParticipantPosition(tx, former.Position.Value, existingParticipantPositions)

					if logAndReturnIfError(err, "Error upserting ParticipantPosition") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantPositions = updatedDirectorBoardPosition

					directorName, updatedDirectorName, err := repository.UpsertParticipantNames(tx, former.Name.Value, existingParticipantNames)

					if logAndReturnIfError(err, "Error upserting ParticipantNames") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantNames = updatedDirectorName

					var capital *float64
					if former.Capital != nil {
						valStr := former.Capital.Value
						if val, err := strconv.ParseFloat(valStr, 64); err == nil {
							capital = &val
						}
					}

					participants := turkey2.Participants{
						NameId:     directorName.Id,
						PositionId: directorBoardPosition.Id,
						Capital:    capital,
						TypeId:     int8(directorBoardId),
					}

					participant, updatedParticipantsList, err := repository.UpsertParticipants(tx, &participants, existingParticipantsList)
					if logAndReturnIfError(err, "Error upserting Participants") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantsList = updatedParticipantsList

					// Company Participants part
					companyParticipants := turkey2.CompanyParticipants{
						ParticipantId: participant.Id,
						CompanyId:     companyId,
					}

					updatedCompanyParticipantsList, err := repository.UpsertCompanyParticipants(tx, &companyParticipants, existingCompanyParticipantsList)
					if logAndReturnIfError(err, "Error updating CompanyParticipants") != nil {
						tx.Rollback()
						return err
					}
					existingCompanyParticipantsList = updatedCompanyParticipantsList
				}
			}

			// former director
			if reply.Data.Units[i].FormerDirectorBoard != nil {
				for _, former := range reply.Data.Units[i].FormerDirectorBoard {
					if former.Position == nil || former.Name == nil || former.Capital == nil {
						continue
					}

					formerDirectorBoardPosition, updatedFormerDirectorBoardPosition, err := repository.UpsertParticipantPosition(tx, former.Position.Value, existingParticipantPositions)

					if logAndReturnIfError(err, "Error upserting ParticipantPosition") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantPositions = updatedFormerDirectorBoardPosition

					formerDirectorName, updatedFormerDirectorName, err := repository.UpsertParticipantNames(tx, former.Name.Value, existingParticipantNames)

					if logAndReturnIfError(err, "Error upserting ParticipantNames") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantNames = updatedFormerDirectorName

					var capital *float64
					if former.Capital != nil {
						valStr := former.Capital.Value
						if val, err := strconv.ParseFloat(valStr, 64); err == nil {
							capital = &val
						}
					}

					participants := turkey2.Participants{
						NameId:     formerDirectorName.Id,
						PositionId: formerDirectorBoardPosition.Id,
						Capital:    capital,
						TypeId:     int8(formerDirectorBoardId),
					}

					participant, updatedParticipantsList, err := repository.UpsertParticipants(tx, &participants, existingParticipantsList)
					if logAndReturnIfError(err, "Error upserting Participants") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantsList = updatedParticipantsList

					// Company Participants part
					companyParticipants := turkey2.CompanyParticipants{
						ParticipantId: participant.Id,
						CompanyId:     companyId,
					}

					updatedCompanyParticipantsList, err := repository.UpsertCompanyParticipants(tx, &companyParticipants, existingCompanyParticipantsList)
					if logAndReturnIfError(err, "Error updating CompanyParticipants") != nil {
						tx.Rollback()
						return err
					}
					existingCompanyParticipantsList = updatedCompanyParticipantsList
				}
			}

			// officials
			if reply.Data.Units[i].Officials != nil {
				for _, former := range reply.Data.Units[i].Officials {
					if former.Task == nil || former.Name == nil {
						continue
					}

					officialPosition, updatedOfficialPosition, err := repository.UpsertParticipantPosition(tx, former.Task.Value, existingParticipantPositions)

					if logAndReturnIfError(err, "Error upserting ParticipantPosition") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantPositions = updatedOfficialPosition

					officialName, updatedOfficialName, err := repository.UpsertParticipantNames(tx, former.Name.Value, existingParticipantNames)

					if logAndReturnIfError(err, "Error upserting ParticipantNames") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantNames = updatedOfficialName

					participants := turkey2.Participants{
						NameId:     officialName.Id,
						PositionId: officialPosition.Id,
						TypeId:     int8(officialStatusId),
					}

					participant, updatedParticipantsList, err := repository.UpsertParticipants(tx, &participants, existingParticipantsList)
					if logAndReturnIfError(err, "Error upserting Participants") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantsList = updatedParticipantsList

					// Company Participants part
					companyParticipants := turkey2.CompanyParticipants{
						ParticipantId: participant.Id,
						CompanyId:     companyId,
					}

					updatedCompanyParticipantsList, err := repository.UpsertCompanyParticipants(tx, &companyParticipants, existingCompanyParticipantsList)
					if logAndReturnIfError(err, "Error upserting CompanyParticipants") != nil {
						tx.Rollback()
						return err
					}
					existingCompanyParticipantsList = updatedCompanyParticipantsList

					if former.EndDate != nil {

						t := former.EndDate.AsTime()
						formerOfficialsDates := turkey2.FormerOfficialsDates{
							ParticipantId: participants.Id,
							EndDate:       &t,
						}

						updatedFormerOfficialsDatesList, err := repository.UpsertFormerOfficialsDates(tx, &formerOfficialsDates, existingFormerOfficialsDatesList)
						if logAndReturnIfError(err, "Error upserting FormerOfficialsDates") != nil {
							tx.Rollback()
							return err
						}
						existingFormerOfficialsDatesList = updatedFormerOfficialsDatesList

					}
				}
			}
			// former officials
			if reply.Data.Units[i].FormerOfficials != nil {
				for _, former := range reply.Data.Units[i].FormerOfficials {
					if former.Task == nil || former.Name == nil {
						continue
					}

					formerOfficialPosition, updatedFormerOfficialPosition, err := repository.UpsertParticipantPosition(tx, former.Task.Value, existingParticipantPositions)

					if logAndReturnIfError(err, "Error upserting ParticipantPosition") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantPositions = updatedFormerOfficialPosition

					formerOfficialName, updatedFormerOfficialName, err := repository.UpsertParticipantNames(tx, former.Name.Value, existingParticipantNames)

					if logAndReturnIfError(err, "Error upserting ParticipantNames") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantNames = updatedFormerOfficialName

					participants := turkey2.Participants{
						NameId:     formerOfficialName.Id,
						PositionId: formerOfficialPosition.Id,
						TypeId:     int8(formerOfficialStatusId),
					}

					participant, updatedParticipantsList, err := repository.UpsertParticipants(tx, &participants, existingParticipantsList)
					if logAndReturnIfError(err, "Error upserting Participants") != nil {
						tx.Rollback()
						return err
					}
					existingParticipantsList = updatedParticipantsList

					// Company Participants part
					companyParticipants := turkey2.CompanyParticipants{
						ParticipantId: participant.Id,
						CompanyId:     companyId,
					}

					updatedCompanyParticipantsList, err := repository.UpsertCompanyParticipants(tx, &companyParticipants, existingCompanyParticipantsList)
					if logAndReturnIfError(err, "Error updating CompanyParticipants") != nil {
						tx.Rollback()
						return err
					}
					existingCompanyParticipantsList = updatedCompanyParticipantsList

					if former.EndDate != nil {

						t := former.EndDate.AsTime()
						formerOfficialsDates := turkey2.FormerOfficialsDates{
							ParticipantId: participants.Id,
							EndDate:       &t,
						}

						updatedFormerOfficialsDatesList, err := repository.UpsertFormerOfficialsDates(tx, &formerOfficialsDates, existingFormerOfficialsDatesList)
						if logAndReturnIfError(err, "Error updating FormerOfficialsDates") != nil {
							tx.Rollback()
							return err
						}
						existingFormerOfficialsDatesList = updatedFormerOfficialsDatesList

					}

				}
			}

			if uin, err := strconv.ParseInt(Uin, 10, 64); err == nil {
				var regDateStr *string
				if reply.Data.Units[i].RegDate != nil {
					s := reply.Data.Units[i].RegDate.String()
					if s != "" {
						regDateStr = &s
					}
				}

				var address *string
				if reply.Data.Units[i].Address != nil {
					s := reply.Data.Units[i].Address.Value
					if s != "" {
						address = &s
					}
				}

				elasticSearch := abstract_db.ElasticSearch{
					Uin:              uin,
					Name:             Name,
					RegistrationDate: regDateStr,
					Address:          address,
				}

				err := pkg3.ElasticsearchInsertORUpdateGeneral(elasticSearch, es, index, strconv.FormatInt(uin, 10))
				if err != nil {
					return err
				}
			}

		}
	}

	if err = tx.Commit().Error; err != nil {
		logger.Error("failed to commit transaction in Turkey service", err.Error())
		return err
	}

	return nil
}
