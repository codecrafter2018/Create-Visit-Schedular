using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using Schedular_for_visit;

namespace Service_codes
{
    /// <summary>
    /// Main program class for managing CRM visit scheduling and account updates.
    /// </summary>
    internal class Program
    {
        /// <summary>
        /// Represents a party entity with relevant properties for visit scheduling.
        /// </summary>
        public class Party
        {
            public Guid party { get; set; }
            public Guid route { get; set; }
            public Guid user { get; set; }
            public string partyName { get; set; }
            public string subject { get; set; }
        }

        /// <summary>
        /// Represents a route entity with associated user information.
        /// </summary>
        public class Route
        {
            public Guid routeId { get; set; }
            public Guid userId { get; set; }
        }

        /// <summary>
        /// Main entry point for the application.
        /// </summary>
        /// <param name="args">Command-line arguments.</param>
        static void Main(string[] args)
        {
            try
            {
                // Establish connection to CRM
                connectCrm getService = new connectCrm();
                CrmServiceClient service = getService.connect();
                Console.WriteLine("Connection Established with CRM");
                Console.WriteLine("Success");

                // Fetch system users with specific roles and region
                string fetchXml = @"
                    <fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'>
                        <entity name='systemuser'>
                            <attribute name='fullname'/>
                            <order attribute='fullname' descending='false'/>
                            <attribute name='systemuserid'/>
                            <attribute name='zox_role'/>
                            <filter type='and'>
                                <condition attribute='zox_role' operator='in'>
                                    <value>100000001</value>
                                    <value>100000004</value>
                                    <value>515140009</value>
                                </condition>
                                <condition attribute='zox_lob' operator='eq' value='100000000'/>
                                <condition attribute='zox_region' operator='eq' value='{regionid}' uiname='regionname' uitype='zox_regionmaster'/>
                            </filter>
                        </entity>
                    </fetch>";

                EntityCollection results = service.RetrieveMultiple(new FetchExpression(fetchXml));
                Console.WriteLine($"User Count: {results.Entities.Count}");

                // Fetch accounts for the specified region
                string fetchXmlRegion1 = @"
                    <fetch version='1.0' mapping='logical' savedqueryid='55beb156-42ab-4227-a7a8-f203df8a6bb8' distinct='true'>
                        <entity name='account'>
                            <attribute name='accountid'/>
                            <attribute name='createdon'/>
                            <attribute name='zox_ytdsaleinlakhs'/>
                            <attribute name='customertypecode'/>
                            <attribute name='zox_lob'/>
                            <attribute name='zox_partyweightage'/>
                            <attribute name='zox_target'/>
                            <attribute name='zox_routemapping'/>
                            <attribute name='zox_visitcount'/>
                            <attribute name='zox_actual'/>
                            <attribute name='zox_saletilllastmonth'/>
                            <filter type='and'>
                                <condition attribute='zox_routemapping' operator='not-null'/>
                                <condition attribute='zox_region' operator='eq' value='{regionid}' uiname='regionname' uitype='zox_regionmaster'/>
                                <condition attribute='customertypecode' operator='in'>
                                    <value>100000002</value>
                                    <value>100000003</value>
                                </condition>
                            </filter>
                            <order attribute='zox_ytdsaleinlakhs' descending='true'/>
                        </entity>
                    </fetch>";

                EntityCollection accountResults = service.RetrieveMultiple(new FetchExpression(fetchXmlRegion1));
                Console.WriteLine($"Total Account: {accountResults.Entities.Count}");

                // Update account records with calculated values
                foreach (var accountEntity in accountResults.Entities)
                {
                    decimal ytdActual = accountEntity.GetAttributeValue<decimal>("zox_actual");
                    decimal ytdSaletill = accountEntity.GetAttributeValue<decimal>("zox_saletilllastmonth");
                    accountEntity["zox_partyweightage"] = 0.0m;
                    accountEntity["zox_visitcount"] = 0;
                    accountEntity["zox_ytdsaleinlakhs"] = new Money(ytdActual + ytdSaletill);
                    service.Update(accountEntity);
                }

                HashSet<Guid> systemUserIds = new HashSet<Guid>();

                // Process each user and their associated routes
                foreach (var entity in results.Entities)
                {
                    Guid systemUserId = entity.GetAttributeValue<Guid>("systemuserid");
                    OptionSetValue userRole = entity.GetAttributeValue<OptionSetValue>("zox_role");
                    string userFilter = systemUserId.ToString();
                    string userName = entity.GetAttributeValue<string>("fullname");
                    Console.WriteLine($"User Id: {userFilter}");
                    systemUserIds.Add(systemUserId);

                    // Update account weightages based on user and role
                    Dictionary<Guid, Decimal> routeWeight1 = AccountUpdateDegrowings(systemUserId, service);
                    Dictionary<Guid, Decimal> routeWeight11 = AccountUpdate(userFilter, service, routeWeight1, userRole);
                    Dictionary<Guid, Decimal> routeWeight = AccountUpdateRetailer(userFilter, service, routeWeight11, userRole);
                    Console.WriteLine($"Route weight count: {routeWeight.Count}");

                    // Fetch user geography mapping
                    string fetchXmlRegion = @"
                        <fetch version='1.0' mapping='logical' savedqueryid='55beb156-42ab-4227-a7a8-f203df8a6bb8' distinct='true'>
                            <entity name='zox_usergeographymapping'>
                                <attribute name='zox_usergeographymappingid'/>
                                <attribute name='createdon'/>
                                <attribute name='zox_user'/>
                                <attribute name='zox_region'/>
                                <attribute name='zox_depot'/>
                                <attribute name='zox_district'/>
                                <filter type='and'>
                                    <condition attribute='zox_user' operator='eq' value='{0}'/>
                                </filter>
                            </entity>
                        </fetch>";

                    fetchXmlRegion = string.Format(fetchXmlRegion, System.Security.SecurityElement.Escape(userFilter));
                    EntityCollection regionResults = service.RetrieveMultiple(new FetchExpression(fetchXmlRegion));
                    EntityReference regionRef = regionResults.Entities.Count > 0
                        ? regionResults.Entities[0].GetAttributeValue<EntityReference>("zox_region")
                        : null;
                    string regionRefId = regionRef?.Id.ToString();

                    Console.WriteLine($"Region Id: {regionRefId} routeWeight.Count: {routeWeight.Count}");

                    if (regionRefId != null && routeWeight.Count > 0)
                    {
                        Dictionary<Guid, int> visitCount = GetTotalVisit(service, regionRefId, userRole);
                        if (visitCount.Count > 0)
                        {
                            Guid visitHeader = CreateVisitHeader(service, visitCount, systemUserId, regionRef.Id, userName);
                            string filterUserRoute = string.Join("", routeWeight.Keys.Select(routeId => $"<value>{routeId}</value>"));

                            foreach (var entry in visitCount)
                            {
                                Console.WriteLine($"Region ID: {entry.Key}, Visit Count: {entry.Value}");
                            }

                            UpdateUserRoute(service, filterUserRoute, routeWeight, userFilter);
                            int totalWorkingDays = GetHoliday(service);
                            Console.WriteLine($"Total Working days: {totalWorkingDays}");
                            int userPerDayVisit = visitCount[regionRef.Id] / totalWorkingDays;
                            int remainingVisit = visitCount[regionRef.Id] % totalWorkingDays;
                            Console.WriteLine($"User per day: {userPerDayVisit}");
                            int totalVisit = visitCount[regionRef.Id];
                            CreateVisits(service, filterUserRoute, userPerDayVisit, totalWorkingDays, userName, visitHeader, userFilter, remainingVisit, totalVisit);
                        }
                        else
                        {
                            Console.WriteLine("Visit not defined for users.");
                        }
                    }
                    else
                    {
                        Console.WriteLine("Account and region not defined for users.");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Main error: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Creates a visit header record for the specified user and region.
        /// </summary>
        /// <param name="service">CRM service client.</param>
        /// <param name="visitCount">Dictionary of visit counts by region.</param>
        /// <param name="user">User ID.</param>
        /// <param name="region">Region ID.</param>
        /// <param name="userName">User name.</param>
        /// <returns>The created visit header ID.</returns>
        private static Guid CreateVisitHeader(IOrganizationService service, Dictionary<Guid, int> visitCount, Guid user, Guid region, string userName)
        {
            try
            {
                DateTime currentDate = DateTime.Today;
                DateTime firstDayOfNextMonth = new DateTime(currentDate.AddMonths(1).Year, currentDate.AddMonths(1).Month, 1);
                string monthName = firstDayOfNextMonth.ToString("MMMM");

                Entity visit = new Entity("zox_visitheader")
                {
                    Attributes =
                    {
                        ["zox_approver"] = new EntityReference("systemuser", user),
                        ["zox_visitcount"] = visitCount[region],
                        ["zox_month"] = monthName,
                        ["zox_name"] = $"{userName}-{monthName}",
                        ["zox_approvalstatus"] = new OptionSetValue(100000000),
                        ["zox_date"] = firstDayOfNextMonth,
                        ["zox_salesperson"] = new EntityReference("systemuser", user)
                    }
                };

                return service.Create(visit);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in CreateVisitHeader: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Updates account weightages for degrowing accounts.
        /// </summary>
        /// <param name="filterCondition">User ID filter.</param>
        /// <param name="service">CRM service client.</param>
        /// <returns>Dictionary of route IDs and their weightages.</returns>
        private static Dictionary<Guid, Decimal> AccountUpdateDegrowings(Guid filterCondition, IOrganizationService service)
        {
            try
            {
                string fetchXmlRegion = @"
                    <fetch version='1.0' mapping='logical' distinct='true'>
                        <entity name='account'>
                            <attribute name='accountid'/>
                            <attribute name='createdon'/>
                            <attribute name='zox_salesinlastmonth'/>
                            <attribute name='zox_personacode'/>
                            <attribute name='zox_lob'/>
                            <attribute name='zox_partyweightage'/>
                            <attribute name='zox_target'/>
                            <attribute name='zox_routemapping'/>
                            <filter type='and'>
                                <condition attribute='ownerid' operator='eq' value='{0}'/>
                                <condition attribute='zox_proratagrowthdegrowthytd' operator='lt' value='-10'/>
                                <condition attribute='zox_routemapping' operator='not-null'/>
                            </filter>
                            <order attribute='zox_proratagrowthdegrowthytd' descending='false'/>
                        </entity>
                    </fetch>";

                fetchXmlRegion = string.Format(fetchXmlRegion, filterCondition);
                Dictionary<Guid, decimal> routeWeightMap = new Dictionary<Guid, decimal>();
                EntityCollection accountResults = service.RetrieveMultiple(new FetchExpression(fetchXmlRegion));
                Console.WriteLine($"Account Count for Degrowing: {accountResults.Entities.Count}");

                int totalRecords = accountResults.Entities.Count;
                int threshold1 = (int)Math.Ceiling(totalRecords * 0.1); // 10%
                Console.WriteLine($"Threshold value 1: {threshold1}");

                int recordCount = 0;
                foreach (var accountEntity in accountResults.Entities)
                {
                    EntityReference routeRef = accountEntity.GetAttributeValue<EntityReference>("zox_routemapping");
                    recordCount++;
                    Guid accountId = accountEntity.GetAttributeValue<Guid>("accountid");
                    Console.WriteLine($"AccountId: {accountId}");

                    decimal weightage = recordCount <= threshold1 ? 4.0m : 0.0m;

                    if (routeWeightMap.ContainsKey(routeRef.Id))
                    {
                        routeWeightMap[routeRef.Id] += weightage;
                    }
                    else
                    {
                        routeWeightMap.Add(routeRef.Id, weightage);
                    }

                    accountEntity["zox_partyweightage"] = weightage;
                    service.Update(accountEntity);
                }

                foreach (var entry in routeWeightMap)
                {
                    Console.WriteLine($"Region ID: {entry.Key}, Visit Count: {entry.Value}");
                }

                return routeWeightMap;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in AccountUpdateDegrowings: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Updates account weightages for dealer accounts based on user role.
        /// </summary>
        /// <param name="filterCondition">User ID filter.</param>
        /// <param name="service">CRM service client.</param>
        /// <param name="routeWeight">Existing route weight dictionary.</param>
        /// <param name="userRole">User role.</param>
        /// <returns>Updated route weight dictionary.</returns>
        private static Dictionary<Guid, Decimal> AccountUpdate(string filterCondition, IOrganizationService service, Dictionary<Guid, Decimal> routeWeight, OptionSetValue userRole)
        {
            try
            {
                string fetchXmlRegion = @"
                    <fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true' savedqueryid='65ffaf9a-e8c5-432d-860b-32f841b00d87' no-lock='false'>
                        <entity name='account'>
                            <attribute name='customertypecode'/>
                            <attribute name='accountid'/>
                            <attribute name='name'/>
                            <attribute name='zox_routemapping'/>
                            <attribute name='zox_partyweightage'/>
                            <filter type='and'>
                                <condition attribute='ownerid' operator='eq' value='{0}'/>
                                <condition attribute='zox_ytdsaleinlakhs' operator='not-null'/>
                                <condition attribute='zox_routemapping' operator='not-null'/>
                                <condition attribute='zox_partyweightage' operator='eq' value='0'/>
                                <condition attribute='customertypecode' operator='eq' value='100000002'/>
                                <condition attribute='zox_region' operator='eq' value='{700c5f7a-e09e-ee11-a569-002248d5d7d5}' uiname='SOUTH GUJARAT' uitype='zox_regionmaster'/>
                            </filter>
                            <order attribute='zox_ytdsaleinlakhs' descending='true'/>
                        </entity>
                    </fetch>";

                fetchXmlRegion = string.Format(fetchXmlRegion, System.Security.SecurityElement.Escape(filterCondition));
                EntityCollection accountResults = service.RetrieveMultiple(new FetchExpression(fetchXmlRegion));
                Console.WriteLine($"Account Count: {accountResults.Entities.Count}");

                int totalRecords = accountResults.Entities.Count;
                int recordCount = 0;

                foreach (var accountEntity in accountResults.Entities)
                {
                    OptionSetValue accountType = accountEntity.GetAttributeValue<OptionSetValue>("customertypecode");
                    EntityReference routeRef = accountEntity.GetAttributeValue<EntityReference>("zox_routemapping");
                    recordCount++;
                    Guid accountId = accountEntity.GetAttributeValue<Guid>("accountid");
                    Console.WriteLine($"AccountType: {accountType.Value} User Role: {userRole.Value}");

                    decimal weightage = 0.0m;
                    if (userRole.Value == 100000001 && accountType.Value == 100000002)
                    {
                        int threshold1 = (int)Math.Ceiling(totalRecords * 0.2);
                        int threshold2 = (int)Math.Ceiling(totalRecords * 0.5);
                        int threshold3 = (int)Math.Ceiling(totalRecords * 0.75);
                        Console.WriteLine($"Threshold value 1: {threshold1}");
                        Console.WriteLine($"Threshold value 2: {threshold2}");
                        Console.WriteLine($"Threshold value 3: {threshold3}");

                        if (recordCount <= threshold1) weightage = 4.0m;
                        else if (recordCount <= threshold2) weightage = 3.0m;
                        else if (recordCount <= threshold3) weightage = 2.0m;
                        else weightage = 1.0m;
                    }
                    else if (userRole.Value == 100000004 && accountType.Value == 100000002)
                    {
                        int threshold1 = (int)Math.Ceiling(totalRecords * 0.2);
                        int threshold2 = (int)Math.Ceiling(totalRecords * 0.5);
                        int threshold3 = (int)Math.Ceiling(totalRecords * 0.75);

                        if (recordCount <= threshold1) weightage = 4.0m;
                        else if (recordCount <= threshold2) weightage = 3.0m;
                        else if (recordCount <= threshold3) weightage = 2.0m;
                    }

                    if (routeWeight.ContainsKey(routeRef.Id))
                    {
                        routeWeight[routeRef.Id] += weightage;
                    }
                    else
                    {
                        routeWeight.Add(routeRef.Id, weightage);
                    }

                    accountEntity["zox_partyweightage"] = weightage;
                    service.Update(accountEntity);
                }

                foreach (var entry in routeWeight)
                {
                    Console.WriteLine($"Region ID in Account: {entry.Key}, Weight value: {entry.Value}");
                }

                return routeWeight;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in AccountUpdate: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Updates account weightages for retailer accounts based on user role.
        /// </summary>
        /// <param name="filterCondition">User ID filter.</param>
        /// <param name="service">CRM service client.</param>
        /// <param name="routeWeight">Existing route weight dictionary.</param>
        /// <param name="userRole">User role.</param>
        /// <returns>Updated route weight dictionary.</returns>
        private static Dictionary<Guid, Decimal> AccountUpdateRetailer(string filterCondition, IOrganizationService service, Dictionary<Guid, Decimal> routeWeight, OptionSetValue userRole)
        {
            try
            {
                string fetchXmlRegion = @"
                    <fetch version='1.0' mapping='logical' savedqueryid='55beb156-42ab-4227-a7a8-f203df8a6bb8' distinct='true'>
                        <entity name='account'>
                            <attribute name='accountid'/>
                            <attribute name='createdon'/>
                            <attribute name='zox_ytdsaleinlakhs'/>
                            <attribute name='customertypecode'/>
                            <attribute name='zox_lob'/>
                            <attribute name='zox_partyweightage'/>
                            <attribute name='zox_target'/>
                            <attribute name='zox_routemapping'/>
                            <filter type='and'>
                                <condition attribute='ownerid' operator='eq' value='{0}'/>
                                <condition attribute='zox_ytdsaleinlakhs' operator='not-null'/>
                                <condition attribute='zox_routemapping' operator='not-null'/>
                                <condition attribute='zox_partyweightage' operator='eq' value='0'/>
                                <condition attribute='customertypecode' operator='eq' value='100000003'/>
                                <condition attribute='zox_region' operator='eq' value='{700c5f7a-e09e-ee11-a569-002248d5d7d5}' uiname='SOUTH GUJARAT' uitype='zox_regionmaster'/>
                            </filter>
                            <order attribute='zox_ytdsaleinlakhs' descending='true'/>
                        </entity>
                    </fetch>";

                fetchXmlRegion = string.Format(fetchXmlRegion, System.Security.SecurityElement.Escape(filterCondition));
                EntityCollection accountResults = service.RetrieveMultiple(new FetchExpression(fetchXmlRegion));
                Console.WriteLine($"Account Count: {accountResults.Entities.Count}");

                int totalRecords = accountResults.Entities.Count;
                int recordCount = 0;

                foreach (var accountEntity in accountResults.Entities)
                {
                    OptionSetValue accountType = accountEntity.GetAttributeValue<OptionSetValue>("customertypecode");
                    EntityReference routeRef = accountEntity.GetAttributeValue<EntityReference>("zox_routemapping");
                    recordCount++;
                    Guid accountId = accountEntity.GetAttributeValue<Guid>("accountid");
                    Console.WriteLine($"AccountId: {accountId}");

                    decimal weightage = 0.0m;
                    if (userRole.Value == 515140009 && accountType.Value == 100000003)
                    {
                        int threshold1 = (int)Math.Ceiling(totalRecords * 0.2);
                        int threshold2 = (int)Math.Ceiling(totalRecords * 0.5);
                        int threshold3 = (int)Math.Ceiling(totalRecords * 0.75);
                        Console.WriteLine($"Threshold value 1: {threshold1}");
                        Console.WriteLine($"Threshold value 2: {threshold2}");
                        Console.WriteLine($"Threshold value 3: {threshold3}");

                        if (recordCount <= threshold1) weightage = 4.0m;
                        else if (recordCount <= threshold2) weightage = 3.0m;
                        else if (recordCount <= threshold3) weightage = 2.0m;
                        else weightage = 1.0m;
                    }
                    else if (userRole.Value == 100000001 && accountType.Value == 100000003)
                    {
                        int threshold1 = (int)Math.Ceiling(totalRecords * 0.3);
                        int threshold2 = (int)Math.Ceiling(totalRecords * 0.5);
                        Console.WriteLine($"Threshold value 1: {threshold1}");
                        Console.WriteLine($"Threshold value 2: {threshold2}");

                        if (recordCount <= threshold1) weightage = 4.0m;
                        else if (recordCount <= threshold2) weightage = 3.0m;
                    }
                    else if (userRole.Value == 100000004 && accountType.Value == 100000003)
                    {
                        int threshold1 = (int)Math.Ceiling(totalRecords * 0.1);
                        Console.WriteLine($"Threshold value 1: {threshold1}");
                        if (recordCount <= threshold1) weightage = 4.0m;
                    }

                    if (routeWeight.ContainsKey(routeRef.Id))
                    {
                        routeWeight[routeRef.Id] += weightage;
                    }
                    else
                    {
                        routeWeight.Add(routeRef.Id, weightage);
                    }

                    accountEntity["zox_partyweightage"] = weightage;
                    service.Update(accountEntity);
                }

                foreach (var entry in routeWeight)
                {
                    Console.WriteLine($"Region ID in Account: {entry.Key}, Weight value: {entry.Value}");
                }

                return routeWeight;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in AccountUpdateRetailer: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Retrieves the total number of visits for a given region and user role.
        /// </summary>
        /// <param name="service">CRM service client.</param>
        /// <param name="filterCondition">Region ID filter.</param>
        /// <param name="userRole">User role.</param>
        /// <returns>Dictionary of region IDs and visit counts.</returns>
        private static Dictionary<Guid, int> GetTotalVisit(IOrganizationService service, string filterCondition, OptionSetValue userRole)
        {
            Dictionary<Guid, int> visitCount = new Dictionary<Guid, int>();
            try
            {
                Console.WriteLine($"{filterCondition} op: {userRole.Value}");
                string fetchXmlRegion = @"
                    <fetch version='1.0' mapping='logical' savedqueryid='55beb156-42ab-4227-a7a8-f203df8a6bb8' distinct='true'>
                        <entity name='zox_compliancematrix'>
                            <attribute name='zox_compliancematrixid'/>
                            <attribute name='createdon'/>
                            <attribute name='zox_name'/>
                            <attribute name='zox_lob'/>
                            <attribute name='zox_region'/>
                            <attribute name='zox_role'/>
                            <attribute name='zox_noofvisit'/>
                            <filter type='and'>
                                <condition attribute='zox_region' operator='eq' value='{0}'/>
                                <condition attribute='zox_role' operator='eq' value='{1}'/>
                                <condition attribute='zox_lob' operator='eq' value='100000000'/>
                            </filter>
                        </entity>
                    </fetch>";

                fetchXmlRegion = string.Format(fetchXmlRegion, System.Security.SecurityElement.Escape(filterCondition), userRole.Value);
                EntityCollection visitRegionResults = service.RetrieveMultiple(new FetchExpression(fetchXmlRegion));
                Console.WriteLine($"Total visit: {visitRegionResults.Entities.Count}");

                foreach (var com in visitRegionResults.Entities)
                {
                    EntityReference regionRef = com.GetAttributeValue<EntityReference>("zox_region");
                    int visitNumber = com.GetAttributeValue<int>("zox_noofvisit");
                    visitCount.Add(regionRef.Id, visitNumber);
                }

                return visitCount;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in GetTotalVisit: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Calculates the total number of working days in the next month, excluding holidays and weekends.
        /// </summary>
        /// <param name="service">CRM service client.</param>
        /// <returns>Total working days.</returns>
        private static int GetHoliday(IOrganizationService service)
        {
            int totalHoliday = 0;
            int counter = 0;
            try
            {
                DateTime currentDate = DateTime.Today;
                DateTime firstDayOfNextMonth = new DateTime(currentDate.AddMonths(1).Year, currentDate.AddMonths(1).Month, 1);
                DateTime lastDayOfNextMonth = firstDayOfNextMonth.AddMonths(1).AddDays(-1);

                string fetchXmlCount = @"
                    <fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                        <entity name='zox_holidaycalendar'>
                            <attribute name='zox_holidaycalendarid'/>
                            <filter type='and'>
                                <condition attribute='zox_date' operator='on-or-after' value='{0}'/>
                                <condition attribute='zox_date' operator='on-or-before' value='{1}'/>
                            </filter>
                        </entity>
                    </fetch>";

                fetchXmlCount = string.Format(fetchXmlCount, firstDayOfNextMonth.ToString("yyyy-MM-dd"), lastDayOfNextMonth.ToString("yyyy-MM-dd"));
                EntityCollection holiday = service.RetrieveMultiple(new FetchExpression(fetchXmlCount));
                totalHoliday = holiday.Entities.Count;

                int saturdayCount = 0;
                int sundayCount = 0;

                for (DateTime date = firstDayOfNextMonth; date <= lastDayOfNextMonth; date = date.AddDays(1))
                {
                    counter++;
                    if (date.DayOfWeek == DayOfWeek.Saturday) saturdayCount++;
                    else if (date.DayOfWeek == DayOfWeek.Sunday) sundayCount++;
                }

                totalHoliday += saturdayCount + sundayCount;
                Console.WriteLine($"Number of holidays: {totalHoliday}");
                counter -= totalHoliday;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in GetHoliday: {ex.Message}");
                throw;
            }

            return counter;
        }

        /// <summary>
        /// Updates user route mappings with weightage points.
        /// </summary>
        /// <param name="service">CRM service client.</param>
        /// <param name="filterUserRoute">Route filter XML.</param>
        /// <param name="routeWeightMap">Route weight dictionary.</param>
        /// <param name="user">User ID.</param>
        private static void UpdateUserRoute(IOrganizationService service, string filterUserRoute, Dictionary<Guid, Decimal> routeWeightMap, string user)
        {
            try
            {
                string fetchXmlRegion = @"
                    <fetch version='1.0' mapping='logical' distinct='true'>
                        <entity name='zox_userroutemapping'>
                            <attribute name='zox_userroutemappingid'/>
                            <attribute name='createdon'/>
                            <attribute name='zox_route'/>
                            <attribute name='zox_weightagepoint'/>
                            <attribute name='zox_user'/>
                            <filter type='and'>
                                <condition attribute='zox_route' operator='in'>{0}</condition>
                                <condition attribute='zox_user' operator='eq' value='{1}'/>
                            </filter>
                        </entity>
                    </fetch>";

                fetchXmlRegion = string.Format(fetchXmlRegion, filterUserRoute, System.Security.SecurityElement.Escape(user));
                EntityCollection userRoute = service.RetrieveMultiple(new FetchExpression(fetchXmlRegion));
                Console.WriteLine($"UserRoute: {userRoute.Entities.Count}");

                foreach (var userRouteEntity in userRoute.Entities)
                {
                    EntityReference routeRef = userRouteEntity.GetAttributeValue<EntityReference>("zox_route");
                    if (routeWeightMap.ContainsKey(routeRef.Id))
                    {
                        userRouteEntity["zox_weightagepoint"] = routeWeightMap[routeRef.Id];
                        service.Update(userRouteEntity);
                    }
                }

                Console.WriteLine("User Route Record Updated Successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in UpdateUserRoute: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Creates visit tasks for the specified user and routes.
        /// </summary>
        /// <param name="service">CRM service client.</param>
        /// <param name="filterUserRoute">Route filter XML.</param>
        /// <param name="visitPerDay">Visits per day.</param>
        /// <param name="totalWorkingDays">Total working days.</param>
        /// <param name="userName">User name.</param>
        /// <param name="visitHeader">Visit header ID.</param>
        /// <param name="user">User ID.</param>
        /// <param name="remainingVisit">Remaining visits.</param>
        /// <param name="totalVisit">Total visits.</param>
        private static void CreateVisits(IOrganizationService service, string filterUserRoute, int visitPerDay, int totalWorkingDays, string userName, Guid visitHeader, string user, int remainingVisit, int totalVisit)
        {
            try
            {
                Dictionary<Guid, List<Party>> routePartyMap = new Dictionary<Guid, List<Party>>();
                string fetchXmlRegion = @"
                    <fetch version='1.0' mapping='logical' distinct='true'>
                        <entity name='zox_userroutemapping'>
                            <attribute name='zox_userroutemappingid'/>
                            <attribute name='createdon'/>
                            <attribute name='zox_route'/>
                            <attribute name='zox_weightagepoint'/>
                            <attribute name='zox_user'/>
                            <filter type='and'>
                                <condition attribute='zox_route' operator='in'>{0}</condition>
                                <condition attribute='zox_user' operator='eq' value='{1}'/>
                                <condition attribute='zox_weightagepoint' operator='gt' value='0'/>
                            </filter>
                            <order attribute='zox_weightagepoint' descending='true'/>
                        </entity>
                    </fetch>";

                fetchXmlRegion = string.Format(fetchXmlRegion, filterUserRoute, System.Security.SecurityElement.Escape(user));
                EntityCollection userRoute = service.RetrieveMultiple(new FetchExpression(fetchXmlRegion));
                Console.WriteLine($"User Route Size: {userRoute.Entities.Count}");

                List<Party> partyList = new List<Party>();
                List<Route> routeList = new List<Route>();

                foreach (var userEntity in userRoute.Entities)
                {
                    EntityReference routeRef = userEntity.GetAttributeValue<EntityReference>("zox_route");
                    string accountRouteId = routeRef.Id.ToString();
                    EntityReference userRef = userEntity.GetAttributeValue<EntityReference>("zox_user");
                    string accountUserId = userRef.Id.ToString();

                    string fetchXmlAccount = @"
                        <fetch version='1.0' mapping='logical' savedqueryid='55beb156-42ab-4227-a7a8-f203df8a6bb8' distinct='true'>
                            <entity name='account'>
                                <attribute name='accountid'/>
                                <attribute name='createdon'/>
                                <attribute name='ownerid'/>
                                <attribute name='zox_routemapping'/>
                                <attribute name='name'/>
                                <filter type='and'>
                                    <condition attribute='zox_routemapping' operator='eq' value='{0}'/>
                                    <condition attribute='ownerid' operator='eq' value='{1}'/>
                                    <condition attribute='zox_ytdsaleinlakhs' operator='not-null'/>
                                    <condition attribute='zox_routemapping' operator='not-null'/>
                                    <condition attribute='zox_partyweightage' operator='gt' value='0'/>
                                    <condition attribute='customertypecode' operator='in'>
                                        <value>100000002</value>
                                        <value>100000003</value>
                                    </condition>
                                </filter>
                                <order attribute='zox_partyweightage' descending='true'/>
                            </entity>
                        </fetch>";

                    fetchXmlAccount = string.Format(fetchXmlAccount, System.Security.SecurityElement.Escape(accountRouteId), System.Security.SecurityElement.Escape(accountUserId));
                    EntityCollection partyRoute = service.RetrieveMultiple(new FetchExpression(fetchXmlAccount));
                    Console.WriteLine($"Party Size with respect to route: {partyRoute.Entities.Count}");

                    int routeCount = partyRoute.Entities.Count / visitPerDay;
                    if (routeCount > 0 && visitPerDay - (partyRoute.Entities.Count % visitPerDay) <= 3)
                    {
                        routeCount++;
                    }
                    else if (routeCount == 0)
                    {
                        routeCount++;
                    }

                    Console.WriteLine($"Total Party In route: {routeCount}");

                    for (int i = 0; i < routeCount; i++)
                    {
                        Route ro = new Route
                        {
                            routeId = partyRoute.Entities[0].GetAttributeValue<EntityReference>("zox_routemapping").Id,
                            userId = partyRoute.Entities[0].GetAttributeValue<EntityReference>("ownerid").Id
                        };
                        routeList.Add(ro);
                    }

                    List<Party> routeParty = new List<Party>();
                    foreach (var partyEntities in partyRoute.Entities)
                    {
                        Party pa = new Party
                        {
                            party = partyEntities.GetAttributeValue<Guid>("accountid"),
                            partyName = partyEntities.GetAttributeValue<string>("name"),
                            subject = $"Visit for {partyEntities.GetAttributeValue<string>("name")}",
                            route = partyEntities.GetAttributeValue<EntityReference>("zox_routemapping").Id,
                            user = partyEntities.GetAttributeValue<EntityReference>("ownerid").Id
                        };
                        partyList.Add(pa);
                        routeParty.Add(pa);
                    }

                    int rouCount = partyRoute.Entities.Count / visitPerDay;
                    if (rouCount > 0 && visitPerDay - (partyRoute.Entities.Count % visitPerDay) <= 3)
                    {
                        int countRoute = visitPerDay - (partyRoute.Entities.Count % visitPerDay);
                        for (int i = 0; i < countRoute; i++)
                        {
                            Party pa = new Party
                            {
                                party = Guid.Empty,
                                partyName = "not defined",
                                subject = "Visit for NON-UTCL",
                                route = partyRoute.Entities[0].GetAttributeValue<EntityReference>("zox_routemapping").Id,
                                user = partyRoute.Entities[0].GetAttributeValue<EntityReference>("ownerid").Id
                            };
                            routeParty.Add(pa);
                        }
                    }
                    else if (rouCount == 0)
                    {
                        int countRoute = visitPerDay - partyRoute.Entities.Count;
                        for (int i = 0; i < countRoute; i++)
                        {
                            Party pa = new Party
                            {
                                party = Guid.Empty,
                                partyName = "not defined",
                                subject = "Visit for NON-UTCL",
                                route = partyRoute.Entities[0].GetAttributeValue<EntityReference>("zox_routemapping").Id,
                                user = partyRoute.Entities[0].GetAttributeValue<EntityReference>("ownerid").Id
                            };
                            routeParty.Add(pa);
                        }
                    }

                    routePartyMap[routeRef.Id] = routeParty;
                    Console.WriteLine($"Route party map size: {routePartyMap.Count}");
                    Console.WriteLine($"Route party on key: {routePartyMap[routeRef.Id]}");
                }

                DateTime currentDate = DateTime.Today;
                DateTime firstDayOfNextMonth = new DateTime(currentDate.AddMonths(1).Year, currentDate.AddMonths(1).Month, 1);
                DateTime lastDayOfNextMonth = firstDayOfNextMonth.AddMonths(1).AddDays(-1);

                int routeCounter = 0;
                Dictionary<Guid, int> partyCount = new Dictionary<Guid, int>();
                int valueCounter = 0;

                for (DateTime date = firstDayOfNextMonth; date <= lastDayOfNextMonth; date = date.AddDays(1))
                {
                    if (date.DayOfWeek != DayOfWeek.Saturday && date.DayOfWeek != DayOfWeek.Sunday && !ReturnHoliday(service, date))
                    {
                        Console.WriteLine($"Route List Size: {routeList.Count}");
                        int counterPart = routeCounter - 1;

                        if (routeList.Count > routeCounter)
                        {
                            if (routeCounter == 0 || routeList[routeCounter].routeId != routeList[counterPart].routeId)
                            {
                                valueCounter = 0;
                            }

                            Entity routeVisit = new Entity("zox_routevisit")
                            {
                                Attributes =
                                {
                                    ["zox_route"] = new EntityReference("zox_route", routeList[routeCounter].routeId),
                                    ["zox_visitheader"] = new EntityReference("zox_visitheader", visitHeader),
                                    ["zox_name"] = userName,
                                    ["zox_date"] = date,
                                    ["ownerid"] = new EntityReference("systemuser", routeList[routeCounter].userId),
                                    ["zox_user"] = new EntityReference("systemuser", routeList[routeCounter].userId)
                                }
                            };
                            service.Create(routeVisit);
                        }
                        else
                        {
                            routeCounter = 0;
                            valueCounter = 0;
                            Entity routeVisit = new Entity("zox_routevisit")
                            {
                                Attributes =
                                {
                                    ["zox_route"] = new EntityReference("zox_route", routeList[routeCounter].routeId),
                                    ["zox_visitheader"] = new EntityReference("zox_visitheader", visitHeader),
                                    ["zox_name"] = userName,
                                    ["zox_date"] = date,
                                    ["ownerid"] = new EntityReference("systemuser", routeList[routeCounter].userId),
                                    ["zox_user"] = new EntityReference("systemuser", routeList[routeCounter].userId)
                                }
                            };
                            service.Create(routeVisit);
                        }

                        List<Party> routePartyList = routePartyMap[routeList[routeCounter].routeId];
                        Console.WriteLine($"routeCounter Value: {routeCounter}");

                        if (!partyCount.ContainsKey(routeList[routeCounter].routeId))
                        {
                            partyCount[routeList[routeCounter].routeId] = 0;
                        }

                        if (routePartyList.Count / visitPerDay > 0 && visitPerDay - (routePartyList.Count % visitPerDay) > 3)
                        {
                            valueCounter = partyCount[routeList[routeCounter].routeId];
                        }

                        routeCounter++;

                        for (int j = 0; j < visitPerDay; j++)
                        {
                            Entity visit = new Entity("task");
                            if (routePartyList[valueCounter].subject != "Visit for NON-UTCL")
                            {
                                visit.Attributes = new AttributeCollection
                                {
                                    ["zox_account"] = new EntityReference("account", routePartyList[valueCounter].party),
                                    ["zox_route"] = new EntityReference("zox_route", routePartyList[valueCounter].route),
                                    ["regardingobjectid"] = new EntityReference("account", routePartyList[valueCounter].party),
                                    ["zox_visitheader"] = new EntityReference("zox_visitheader", visitHeader),
                                    ["subject"] = routePartyList[valueCounter].subject,
                                    ["zox_planstatus"] = new OptionSetValue(100000000),
                                    ["zox_visitdate"] = date,
                                    ["ownerid"] = new EntityReference("systemuser", routePartyList[valueCounter].user)
                                };
                            }
                            else
                            {
                                visit.Attributes = new AttributeCollection
                                {
                                    ["zox_route"] = new EntityReference("zox_route", routePartyList[valueCounter].route),
                                    ["zox_visitheader"] = new EntityReference("zox_visitheader", visitHeader),
                                    ["subject"] = routePartyList[valueCounter].subject,
                                    ["zox_visitdate"] = date,
                                    ["ownerid"] = new EntityReference("systemuser", routePartyList[valueCounter].user),
                                    ["zox_tasktype"] = new OptionSetValue(100000002),
                                    ["zox_othertasktype"] = new OptionSetValue(100000005)
                                };
                            }

                            Guid entityId = service.Create(visit);
                            Console.WriteLine(entityId);

                            if (routePartyList.Count - valueCounter == 1)
                            {
                                valueCounter = 0;
                                partyCount[routePartyList[valueCounter].route] = valueCounter;
                                continue;
                            }

                            valueCounter++;
                            partyCount[routePartyList[valueCounter].route] = valueCounter;
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Processing day {date:yyyy-MM-dd}");
                    }
                }

                if (remainingVisit > 0)
                {
                    CreateRemainingVisit(service, visitHeader, user, remainingVisit, firstDayOfNextMonth, lastDayOfNextMonth, totalVisit, totalWorkingDays);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in CreateVisits: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Creates remaining visit tasks for NON-UTCL accounts.
        /// </summary>
        /// <param name="service">CRM service client.</param>
        /// <param name="visitHeader">Visit header ID.</param>
        /// <param name="user">User ID.</param>
        /// <param name="remainingVisit">Remaining visits.</param>
        /// <param name="firstDayOfNextMonth">First day of next month.</param>
        /// <param name="lastDayOfNextMonth">Last day of next month.</param>
        /// <param name="totalVisit">Total visits.</param>
        /// <param name="totalWorkingDays">Total working days.</param>
        private static void CreateRemainingVisit(IOrganizationService service, Guid visitHeader, string user, int remainingVisit, DateTime firstDayOfNextMonth, DateTime lastDayOfNextMonth, int totalVisit, int totalWorkingDays)
        {
            try
            {
                for (DateTime date = firstDayOfNextMonth; date <= lastDayOfNextMonth && remainingVisit > 0; date = date.AddDays(1))
                {
                    if (date.DayOfWeek != DayOfWeek.Saturday && date.DayOfWeek != DayOfWeek.Sunday && !ReturnHoliday(service, date))
                    {
                        EntityCollection routeVisit = GetRoute(service, date, visitHeader);
                        EntityReference route = routeVisit.Entities[0].GetAttributeValue<EntityReference>("zox_route");
                        Guid routeId = route.Id;
                        EntityReference userId = routeVisit.Entities[0].GetAttributeValue<EntityReference>("zox_user");
                        Guid useId = userId.Id;

                        Entity visit = new Entity("task")
                        {
                            Attributes =
                            {
                                ["zox_route"] = new EntityReference("zox_route", routeId),
                                ["zox_visitheader"] = new EntityReference("zox_visitheader", visitHeader),
                                ["subject"] = "Visit for NON-UTCL",
                                ["zox_visitdate"] = date,
                                ["ownerid"] = new EntityReference("systemuser", useId),
                                ["zox_tasktype"] = new OptionSetValue(100000002),
                                ["zox_othertasktype"] = new OptionSetValue(100000005)
                            }
                        };

                        Guid entityId = service.Create(visit);
                        Console.WriteLine(entityId);
                        remainingVisit--;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in CreateRemainingVisit: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Updates visit counts for accounts.
        /// </summary>
        /// <param name="service">CRM service client.</param>
        /// <param name="partyCountMap">Dictionary of account IDs and visit counts.</param>
        private static void AccountVisitCount(IOrganizationService service, Dictionary<Guid, int> partyCountMap)
        {
            try
            {
                List<Guid> guidsToCheck = partyCountMap.Keys.ToList();
                Console.WriteLine($"PartyCountMap: {partyCountMap.Count}");

                QueryExpression query = new QueryExpression("account")
                {
                    ColumnSet = new ColumnSet("accountid", "zox_visitcount"),
                    Criteria = new FilterExpression
                    {
                        Conditions = { new ConditionExpression("accountid", ConditionOperator.In, guidsToCheck.Cast<object>().ToArray()) }
                    }
                };

                EntityCollection accounts = service.RetrieveMultiple(query);
                foreach (var accountEntity in accounts.Entities)
                {
                    Guid accountId = accountEntity.GetAttributeValue<Guid>("accountid");
                    accountEntity["zox_visitcount"] = partyCountMap[accountId];
                    service.Update(accountEntity);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in AccountVisitCount: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Checks if a given date is a holiday.
        /// </summary>
        /// <param name="service">CRM service client.</param>
        /// <param name="dateValue">Date to check.</param>
        /// <returns>True if the date is a holiday, false otherwise.</returns>
        private static bool ReturnHoliday(IOrganizationService service, DateTime dateValue)
        {
            try
            {
                string formattedToday = dateValue.ToString("yyyy-MM-dd");
                string fetchXmlCount = @"
                    <fetch version='1.0' mapping='logical' savedqueryid='5793195f-0c79-4795-8608-3944065d2deb' no-lock='false' distinct='true'>
                        <entity name='zox_holidaycalendar'>
                            <attribute name='statecode'/>
                            <attribute name='zox_holidaycalendarid'/>
                            <attribute name='zox_name'/>
                            <attribute name='createdon'/>
                            <attribute name='zox_date'/>
                            <attribute name='ownerid'/>
                            <attribute name='zox_enddate'/>
                            <filter type='and'>
                                <condition attribute='zox_date' operator='on-or-after' value='{0}'/>
                                <condition attribute='zox_date' operator='on-or-before' value='{0}'/>
                            </filter>
                        </entity>
                    </fetch>";

                fetchXmlCount = string.Format(fetchXmlCount, formattedToday);
                EntityCollection partyRoute = service.RetrieveMultiple(new FetchExpression(fetchXmlCount));
                Console.WriteLine($"dateValue: {formattedToday}");
                Console.WriteLine($"Count of holiday in this month: {partyRoute.Entities.Count}");

                return partyRoute.Entities.Count > 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in ReturnHoliday: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Retrieves route visits for a specific date and visit header.
        /// </summary>
        /// <param name="service">CRM service client.</param>
        /// <param name="date">Date to query.</param>
        /// <param name="visitHeader">Visit header ID.</param>
        /// <returns>Collection of route visit entities.</returns>
        private static EntityCollection GetRoute(IOrganizationService service, DateTime date, Guid visitHeader)
        {
            var query = new QueryExpression("zox_routevisit")
            {
                Distinct = true,
                ColumnSet = new ColumnSet("zox_routevisitid", "zox_route", "zox_date", "zox_visitheader", "zox_user"),
                Criteria = new FilterExpression
                {
                    Conditions =
                    {
                        new ConditionExpression("zox_date", ConditionOperator.Equal, date.Date),
                        new ConditionExpression("zox_visitheader", ConditionOperator.Equal, visitHeader)
                    }
                }
            };

            return service.RetrieveMultiple(query);
        }

        /// <summary>
        /// Retrieves accounts for a specific route with non-zero weightage.
        /// </summary>
        /// <param name="service">CRM service client.</param>
        /// <param name="routeId">Route ID.</param>
        /// <returns>Collection of account entities.</returns>
        private static EntityCollection GetAccount(IOrganizationService service, Guid routeId)
        {
            var query = new QueryExpression("account")
            {
                Distinct = true,
                TopCount = 1,
                ColumnSet = new ColumnSet("accountid", "zox_visitcount", "zox_routemapping", "zox_partyweightage", "ownerid", "name"),
                Criteria = new FilterExpression
                {
                    Conditions =
                    {
                        new ConditionExpression("zox_routemapping", ConditionOperator.Equal, routeId),
                        new ConditionExpression("zox_partyweightage", ConditionOperator.NotEqual, 0)
                    }
                }
            };
            query.AddOrder("zox_visitcount", OrderType.Ascending);

            return service.RetrieveMultiple(query);
        }
    }
}