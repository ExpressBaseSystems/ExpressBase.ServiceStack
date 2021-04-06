using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Enums;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Objects.WebFormRelated;
using Newtonsoft.Json;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class LeadManagementService : EbBaseService
    {
        public LeadManagementService(IEbConnectionFactory _dbf) : base(_dbf) { }

        public GetManageLeadResponse Any(GetManageLeadRequest request)
        {
            string SqlQry = $@"SELECT id, longname FROM eb_locations WHERE id > 0;
							  SELECT id, name FROM hoc_staff WHERE type='doctor' AND COALESCE(eb_del, 'F')='F' ORDER BY name;
							  SELECT id, INITCAP(TRIM(fullname)), statusid FROM eb_users WHERE id > 1 ORDER BY fullname;
							SELECT DISTINCT INITCAP(TRIM(clcity)) AS clcity FROM customers WHERE LENGTH(clcity) > 2 ORDER BY clcity;
							SELECT DISTINCT INITCAP(TRIM(clcountry)) AS clcountry FROM customers WHERE LENGTH(clcountry) > 2 ORDER BY clcountry;
							SELECT DISTINCT INITCAP(TRIM(city)) AS city FROM customers WHERE LENGTH(city) > 2 ORDER BY city;
							SELECT district FROM lead_district WHERE COALESCE(eb_del, 'F')='F' ORDER BY district;
							SELECT source FROM lead_source WHERE COALESCE(eb_del, 'F')='F' ORDER BY source;
							SELECT DISTINCT INITCAP(TRIM(subcategory)) AS subcategory FROM customers WHERE LENGTH(subcategory) > 2 ORDER BY subcategory;
							SELECT status,nextstatus FROM lead_status WHERE COALESCE(eb_del, 'F')='F' ORDER BY status;
							SELECT service FROM lead_service WHERE COALESCE(eb_del, 'F')='F' ORDER BY order_id;
							SELECT id, name FROM hoc_staff WHERE type='nurse' AND COALESCE(eb_del, 'F')='F' ORDER BY name;
							SELECT id, category FROM customer_category WHERE COALESCE(eb_del, 'F')='F';";
            List<DbParameter> paramList = new List<DbParameter>();
            Dictionary<int, string> CostCenter = new Dictionary<int, string>();
            Dictionary<string, int> DocDict = new Dictionary<string, int>();
            List<StaffInfo> StaffInfoAll = new List<StaffInfo>();
            Dictionary<string, int> StaffDict = new Dictionary<string, int>();
            Dictionary<string, int> NurseDict = new Dictionary<string, int>();
            Dictionary<string, string> CustomerData = new Dictionary<string, string>();
            Dictionary<int, string> customercategoryDict = new Dictionary<int, string>();
            List<string> clcityList = new List<string>();
            List<string> clcountryList = new List<string>();
            List<string> cityList = new List<string>();
            List<string> districtList = new List<string>();
            List<string> sourcecategoryList = new List<string>();
            List<string> subcategoryList = new List<string>();
            Dictionary<string, string> statusDict = new Dictionary<string, string>();
            List<string> serviceList = new List<string>();
            List<FeedbackEntry> Flist = new List<FeedbackEntry>();
            List<BillingEntry> Blist = new List<BillingEntry>();
            List<SurgeryEntry> Slist = new List<SurgeryEntry>();
            string attachImgInfo = "[]", prpImgInfo = "[]";

            int Mode = 0;
            if (request.RequestMode == 1)//edit mode 
            {
                SqlQry += @"SELECT id, eb_loc_id, trdate, genurl, name, dob, genphoffice, profession, genemail, customertype, clcity, clcountry, city,
								typeofcustomer, sourcecategory, subcategory, consultation, online_consultation, picsrcvd, dprefid, sex, district, leadowner,
                                baldnessgrade, diffusepattern, hfcurrently, htpreviously, country_code, watsapp_phno, cust_category, eb_modifiedby 
								FROM customers WHERE id = :accountid AND COALESCE(eb_del, 'F')='F';
							SELECT id,trdate,status,followupdate,narration, eb_createdby, eb_createddt,isnotpickedup FROM leaddetails
								WHERE customers_id=:accountid AND COALESCE(eb_del, 'F')='F' ORDER BY eb_createddt DESC;
							SELECT id,trdate,totalamount,advanceamount,balanceamount,cashreceived,paymentmode,bank,createddt,narration,createdby 
								FROM leadpaymentdetails WHERE customers_id=:accountid AND COALESCE(eb_del, 'F')='F' ORDER BY balanceamount;
							SELECT id,dateofsurgery,eb_loc_id,createdby,createddt, extractiondone_by,
									implantation_by,consent_by,anaesthesia_by,post_briefing_by,nurses_id,complementry,method,narration
								FROM leadsurgerystaffdetails WHERE customers_id=:accountid AND COALESCE(eb_del, 'F')='F' ORDER BY createddt DESC;
							SELECT noofgrafts,totalrate,prpsessions,consulted,consultingfeepaid,consultingdoctor,eb_closing,LOWER(TRIM(nature)),consdate,probmonth
								FROM leadratedetails WHERE customers_id=:accountid;";

                SqlQry += $@"SELECT B.id, B.filename, B.tags, B.uploadts, B.filecategory
								FROM eb_files_ref B LEFT JOIN customer_files A 
								ON A.eb_files_ref_id = B.id				
								WHERE ((A.customer_id = :accountid AND COALESCE(A.eb_del, false) = false) OR B.context_sec = 'CustomerId:{request.AccId}')
								AND COALESCE(B.eb_del, 'F') = 'F' AND COALESCE(B.context, '') <> 'prp';
							
							SELECT B.id, B.filename, B.tags, B.uploadts, B.filecategory
								FROM eb_files_ref B LEFT JOIN customer_files A 
								ON A.eb_files_ref_id = B.id				
								WHERE ((A.customer_id = :accountid AND COALESCE(A.eb_del, false) = false) OR B.context_sec = 'Prp_CustomerId:{request.AccId}')
								AND COALESCE(B.eb_del, 'F') = 'F' AND B.context = 'prp';

                            SELECT B.id, B.filename, B.tags, B.uploadts, B.filecategory
                                FROM customers A, eb_files_ref B 
                                WHERE A.id = :accountid AND COALESCE(A.eb_del, 'F')='F' AND
                                B.context_sec = 'CustomerPhNo:' || A.genurl AND COALESCE(B.eb_del, 'F') = 'F';";

                //SELECT eb_files_ref_id
                //    FROM customer_files WHERE customer_id = :accountid AND eb_del = false;

                //SELECT 
                //	B.id, B.filename, B.tags 
                //FROM
                //	customer_files A,
                //	eb_files_ref B
                //WHERE
                //	A.eb_files_ref_id = B.id AND
                //	A.customer_id = :accountid;
                paramList.Add(this.EbConnectionFactory.DataDB.GetNewParameter("accountid", EbDbTypes.Int32, request.AccId));
            }
            var ds = this.EbConnectionFactory.DataDB.DoQueries(SqlQry, paramList.ToArray());

            foreach (var dr in ds.Tables[0].Rows)
                CostCenter.Add(Convert.ToInt32(dr[0]), dr[1].ToString());
            foreach (var dr in ds.Tables[1].Rows)
            {
                if (!DocDict.ContainsKey(dr[1].ToString()))
                    DocDict.Add(dr[1].ToString(), Convert.ToInt32(dr[0]));
                else if (!DocDict.ContainsKey(dr[1].ToString() + "."))
                    DocDict.Add(dr[1].ToString() + ".", Convert.ToInt32(dr[0]));
            }
            foreach (var dr in ds.Tables[2].Rows)
            {
                StaffInfo item = new StaffInfo()
                {
                    id = Convert.ToInt32(dr[0]),
                    name = dr[1].ToString(),
                    status = Convert.ToInt32(dr[2])
                };
                StaffInfoAll.Add(item);
                if (item.status == 0)
                {
                    if (!StaffDict.ContainsKey(item.name))
                        StaffDict.Add(item.name, item.id);
                }
            }
            foreach (var dr in ds.Tables[11].Rows)
            {
                if (!NurseDict.ContainsKey(dr[1].ToString()))
                    NurseDict.Add(dr[1].ToString(), Convert.ToInt32(dr[0]));
                else if (!NurseDict.ContainsKey(dr[1].ToString() + "."))
                    NurseDict.Add(dr[1].ToString() + ".", Convert.ToInt32(dr[0]));
            }
            foreach (var dr in ds.Tables[12].Rows)
                if (!customercategoryDict.ContainsKey(Convert.ToInt32(dr[0])))
                    customercategoryDict.Add(Convert.ToInt32(dr[0]), dr[1].ToString());

            foreach (var dr in ds.Tables[3].Rows)
                clcityList.Add(dr[0].ToString());
            foreach (var dr in ds.Tables[4].Rows)
                clcountryList.Add(dr[0].ToString());
            foreach (var dr in ds.Tables[5].Rows)
                cityList.Add(dr[0].ToString());
            foreach (var dr in ds.Tables[6].Rows)
                districtList.Add(dr[0].ToString());

            foreach (var dr in ds.Tables[7].Rows)
                sourcecategoryList.Add(dr[0].ToString());
            foreach (var dr in ds.Tables[8].Rows)
                subcategoryList.Add(dr[0].ToString());
            foreach (var dr in ds.Tables[9].Rows)
                if (!statusDict.ContainsKey(dr[0].ToString()))
                    statusDict.Add(dr[0].ToString(), dr[1].ToString());
            foreach (var dr in ds.Tables[10].Rows)
                serviceList.Add(dr[0].ToString());

            int Qcnt = 13;//Query count first part
            if (ds.Tables.Count > Qcnt && ds.Tables[Qcnt].Rows.Count > 0)
            {
                Mode = 1;
                var dr = ds.Tables[Qcnt].Rows[0];
                CustomerData.Add("accountid", dr[0].ToString());
                CustomerData.Add("eb_loc_id", dr[1].ToString());
                CustomerData.Add("trdate", getStringValue(dr[2]));
                CustomerData.Add("genurl", dr[3].ToString());
                CustomerData.Add("name", dr[4].ToString());
                CustomerData.Add("dob", getStringValue(dr[5]));
                //CustomerData.Add("age", dr[6].ToString());
                CustomerData.Add("genphoffice", dr[6].ToString());
                CustomerData.Add("profession", dr[7].ToString());
                CustomerData.Add("genemail", dr[8].ToString());
                CustomerData.Add("customertype", dr[9].ToString());
                CustomerData.Add("clcity", dr[10].ToString());
                CustomerData.Add("clcountry", dr[11].ToString());
                CustomerData.Add("city", dr[12].ToString());
                CustomerData.Add("typeofcustomer", dr[13].ToString());
                CustomerData.Add("sourcecategory", dr[14].ToString());
                CustomerData.Add("subcategory", dr[15].ToString());
                CustomerData.Add("consultation", dr[16].ToString().ToLower());
                CustomerData.Add("online_consultation", dr[17].ToString().ToLower());
                CustomerData.Add("picsrcvd", dr[18].ToString().ToLower());
                CustomerData.Add("dprefid", dr[19].ToString());
                CustomerData.Add("sex", dr[20].ToString());
                CustomerData.Add("district", dr[21].ToString());
                CustomerData.Add("leadowner", dr[22].ToString());
                TryInsert(dr[22].ToString(), StaffDict, StaffInfoAll);
                CustomerData.Add("baldnessgrade", dr[23].ToString());
                CustomerData.Add("diffusepattern", dr[24].ToString().ToLower());
                CustomerData.Add("hfcurrently", dr[25].ToString().ToLower());
                CustomerData.Add("htpreviously", dr[26].ToString().ToLower());
                CustomerData.Add("country_code", dr[27].ToString());
                CustomerData.Add("watsapp_phno", dr[28].ToString());
                CustomerData.Add("cust_category", dr[29].ToString());
                int uid = Convert.ToInt32(dr[30]);
                StaffInfo sinfo = StaffInfoAll.Find(e => e.id == uid);
                CustomerData.Add("eb_modifiedby", sinfo == null ? string.Empty : sinfo.name);

                if (ds.Tables[Qcnt + 4].Rows.Count > 0)
                {
                    dr = ds.Tables[Qcnt + 4].Rows[0];
                    CustomerData.Add("noofgrafts", dr[0].ToString());
                    CustomerData.Add("totalrate", dr[1].ToString());
                    CustomerData.Add("prpsessions", dr[2].ToString());
                    //CustomerData.Add("consulted", dr[3].ToString().ToLower());
                    CustomerData.Add("consultingfeepaid", dr[4].ToString().ToLower());
                    CustomerData.Add("consultingdoctor", dr[5].ToString());
                    CustomerData.Add("eb_closing", dr[6].ToString());
                    TryInsert(dr[6].ToString(), StaffDict, StaffInfoAll);
                    CustomerData.Add("nature", dr[7].ToString());
                    CustomerData.Add("consdate", getStringValue(dr[8]));
                    CustomerData.Add("probmonth", (string.IsNullOrEmpty(getStringValue(dr[9])) ? string.Empty : getStringValue(dr[9]).Substring(3).Replace("-", "/")));
                }

                List<FileMetaInfo> _list = new List<FileMetaInfo>();
                foreach (EbDataRow dRow in ds.Tables[Qcnt + 5].Rows)
                {
                    FileMetaInfo info = new FileMetaInfo
                    {
                        FileRefId = Convert.ToInt32(dRow[0]),
                        FileName = Convert.ToString(dRow[1]),
                        Meta = JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(Convert.ToString(dRow[2])),
                        UploadTime = getStringValue(dRow[3], true, true),
                        FileCategory = (EbFileCategory)Convert.ToInt32(dRow[4])
                    };

                    if (!_list.Contains(info))
                        _list.Add(info);
                }
                foreach (EbDataRow dRow in ds.Tables[Qcnt + 7].Rows)
                {
                    FileMetaInfo info = new FileMetaInfo
                    {
                        FileRefId = Convert.ToInt32(dRow[0]),
                        FileName = Convert.ToString(dRow[1]),
                        Meta = JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(Convert.ToString(dRow[2])),
                        UploadTime = getStringValue(dRow[3], true, true),
                        FileCategory = (EbFileCategory)Convert.ToInt32(dRow[4])
                    };

                    if (!_list.Contains(info))
                        _list.Add(info);
                }
                attachImgInfo = JsonConvert.SerializeObject(_list);

                _list = new List<FileMetaInfo>();
                foreach (EbDataRow dRow in ds.Tables[Qcnt + 6].Rows)
                {
                    FileMetaInfo info = new FileMetaInfo
                    {
                        FileRefId = Convert.ToInt32(dRow[0]),
                        FileName = Convert.ToString(dRow[1]),
                        Meta = JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(Convert.ToString(dRow[2])),
                        UploadTime = getStringValue(dRow[3], true, true),
                        FileCategory = (EbFileCategory)Convert.ToInt32(dRow[4])
                    };

                    if (!_list.Contains(info))
                        _list.Add(info);
                }
                prpImgInfo = JsonConvert.SerializeObject(_list);

                //followup details
                foreach (var i in ds.Tables[Qcnt + 1].Rows)
                {
                    uid = Convert.ToInt32(i[5]);
                    sinfo = StaffInfoAll.Find(e => e.id == uid);
                    Flist.Add(new FeedbackEntry
                    {
                        Id = Convert.ToInt32(i[0]),
                        Date = getStringValue(i[1]),
                        Status = i[2].ToString(),
                        Fup_Date = getStringValue(i[3]),
                        Comments = i[4].ToString(),
                        Created_By = sinfo == null ? string.Empty : sinfo.name,
                        Created_Date = getStringValue(i[6], true, true),
                        Is_Picked_Up = Convert.ToBoolean(i[7]) ? "No" : "Yes"
                    });
                }

                //Billing details
                foreach (var i in ds.Tables[Qcnt + 2].Rows)
                {
                    int cash_paid = Convert.ToInt32(i[5]);
                    Blist.Add(new BillingEntry
                    {
                        Id = Convert.ToInt32(i[0]),
                        Date = getStringValue(i[1]),
                        Total_Amount = Convert.ToInt32(i[2]),
                        Total_Received = Convert.ToInt32(i[3]) + cash_paid,
                        Balance_Amount = Convert.ToInt32(i[4]) - cash_paid,
                        Cash_Paid = cash_paid,
                        Payment_Mode = i[6].ToString(),
                        Bank = i[7].ToString(),
                        Clearence_Date = getStringValue(i[8]),
                        Narration = i[9].ToString(),
                        Created_By = i[10].ToString()
                    });
                }

                //surgery details
                foreach (var i in ds.Tables[Qcnt + 3].Rows)
                {
                    Slist.Add(new SurgeryEntry
                    {
                        Id = Convert.ToInt32(i[0]),
                        Date = getStringValue(i[1]),
                        Branch = CostCenter[Convert.ToInt32(i[2])],
                        Created_By = i[3].ToString(),
                        Created_Date = getStringValue(i[4]),
                        Extract_By = Convert.ToInt32(i[5]),
                        Implant_By = Convert.ToInt32(i[6]),
                        Consent_By = Convert.ToInt32(i[7]),
                        Anaesthesia_By = Convert.ToInt32(i[8]),
                        Post_Brief_By = Convert.ToInt32(i[9]),
                        Nurse = Convert.ToInt32(i[10]),
                        Complimentary = i[11].ToString(),
                        Method = i[12].ToString(),
                        Comment = i[13].ToString()
                    });
                }
            }


            return new GetManageLeadResponse
            {
                RespMode = Mode,
                CostCenterDict = CostCenter,
                DoctorDict = DocDict,
                StaffDict = StaffDict,
                CustomerDataDict = CustomerData,
                FeedbackList = Flist,
                BillingList = Blist,
                SurgeryList = Slist,
                CrntCityList = clcityList,
                CrntCountryList = clcountryList,
                CityList = cityList,
                DistrictList = districtList,
                SourceCategoryList = sourcecategoryList,
                SubCategoryList = subcategoryList,
                StatusDict = statusDict,
                ServiceList = serviceList,
                NurseDict = NurseDict,
                CustomerCategoryDict = customercategoryDict,
                AttachImgInfo = attachImgInfo,
                PrpImgInfo = prpImgInfo
            };
        }

        private void TryInsert(string val, Dictionary<string, int> StaffDict, List<StaffInfo> StaffInfoAll)
        {
            int.TryParse(val, out int uid);
            if (!StaffDict.ContainsValue(uid))
            {
                StaffInfo sinfo = StaffInfoAll.Find(e => e.id == uid);
                if (sinfo != null)
                {
                    if (!StaffDict.ContainsKey(sinfo.name))
                        StaffDict.Add(sinfo.name, sinfo.id);
                }
            }
        }

        private class StaffInfo
        {
            public int id { get; set; }
            public string name { get; set; }
            public int status { get; set; }
        }

        //     public GetImageInfoResponse Any(GetImageInfoRequest request)///
        //     {

        ////            string Qry = $@"
        ////SELECT 
        ////	B.id, B.filename, B.tags, B.uploadts
        ////FROM
        ////	customer_files A,
        ////	eb_files_ref B
        ////WHERE
        ////	(A.eb_files_ref_id = B.id AND
        ////	A.customer_id = :accountid AND A.eb_del = false) OR B.context_sec = 'CustomersId:{request.CustomerId}';";
        //string Qry = $@"
        //SELECT B.id, B.filename, B.tags, B.uploadts
        //	FROM eb_files_ref B LEFT JOIN customer_files A 
        //	ON A.eb_files_ref_id = B.id				
        //WHERE ((A.customer_id = :accountid AND A.eb_del = false) OR B.context_sec = 'CustomerId:{request.CustomerId}')
        //	AND COALESCE(B.eb_del, 'F') = 'F';";

        //List<FileMetaInfo> _list = new List<FileMetaInfo>();

        //         DbParameter[] param = new DbParameter[]
        //         {
        //             this.EbConnectionFactory.DataDB.GetNewParameter("accountid", EbDbTypes.Int32, request.CustomerId)
        //         };

        //         var dt = this.EbConnectionFactory.DataDB.DoQuery(Qry, param);

        //         foreach(EbDataRow dr in dt.Rows)
        //         {
        //  	string tags = string.IsNullOrEmpty(dr["tags"] as string) ? "{}" : (dr["tags"] as string);
        //             FileMetaInfo info = new FileMetaInfo
        //             {
        //                 FileRefId = Convert.ToInt32(dr["id"]),
        //                 FileName = dr["filename"] as string,
        //                 Meta = JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(tags),
        //                 UploadTime = Convert.ToDateTime(dr["uploadts"]).ToString("dd-MM-yyyy hh:mm tt")
        //             };

        //             if (!_list.Contains(info))
        //                 _list.Add(info);
        //         }

        //         return new GetImageInfoResponse { Data = _list };
        //     }

        private string getStringValue(object obj)
        {
            obj = (obj == DBNull.Value) ? DateTime.MinValue : obj;
            return (((DateTime)obj).Date != DateTime.MinValue) ? Convert.ToDateTime(obj).ToString("dd-MM-yyyy") : string.Empty;
        }

        private string getStringValue(object obj, bool includetime, bool tolocal)
        {
            obj = (obj == DBNull.Value) ? DateTime.MinValue : obj;
            string format = "dd-MM-yyyy";
            TimeSpan timeSpan = new TimeSpan(0, 0, 0);
            if (includetime)
                format = "dd-MM-yyyy hh:mm tt";
            if (tolocal)
                timeSpan = new TimeSpan(5, 30, 0);

            return (((DateTime)obj).Date != DateTime.MinValue) ? Convert.ToDateTime(obj).Add(timeSpan).ToString(format) : string.Empty;
        }

        private void GetParameter(Dictionary<string, KeyValueType_Field> dict, string key, EbDbTypes ebDbTypes, List<DbParameter> parameters, ref string cols, ref string vals, ref string upcolsvals)
        {
            if (dict.ContainsKey(key))
            {
                object value;
                if (ebDbTypes == EbDbTypes.Int32)
                    value = Convert.ToInt32(dict[key].Value);
                else if (ebDbTypes == EbDbTypes.BooleanOriginal)
                    value = Convert.ToBoolean(dict[key].Value);
                else if (ebDbTypes == EbDbTypes.Date)
                {
                    if (key == "probmonth")
                        value = Convert.ToDateTime(DateTime.ParseExact(dict[key].Value.ToString(), "MM/yyyy", CultureInfo.InvariantCulture));
                    else
                        value = Convert.ToDateTime(DateTime.ParseExact(dict[key].Value.ToString(), "dd-MM-yyyy", CultureInfo.InvariantCulture));
                }
                else
                    value = dict[key].Value;

                parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter(key, ebDbTypes, value));
                cols += $"{key},";
                vals += $":{key},";
                upcolsvals += $"{key}=:{key},";
            }
        }

        public SaveCustomerResponse Any(SaveCustomerRequest request)
        {
            List<KeyValueType_Field> Fields = JsonConvert.DeserializeObject<List<KeyValueType_Field>>(request.CustomerData);
            var dict = Fields.ToDictionary(x => x.Key);
            KeyValueType_Field found;
            List<DbParameter> parameters = new List<DbParameter>();
            List<DbParameter> parameters2 = new List<DbParameter>();
            string cols = string.Empty, vals = string.Empty;
            string cols2 = string.Empty, vals2 = string.Empty;
            string upcolsvals = string.Empty;
            string upcolsvals2 = string.Empty;

            GetParameter(dict, "eb_loc_id", EbDbTypes.Int32, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "trdate", EbDbTypes.Date, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "genurl", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "name", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "dob", EbDbTypes.Date, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "genphoffice", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "watsapp_phno", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "profession", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "genemail", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "customertype", EbDbTypes.Int32, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "clcity", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "clcountry", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "city", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "typeofcustomer", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "sourcecategory", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "subcategory", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "consultation", EbDbTypes.BooleanOriginal, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "online_consultation", EbDbTypes.BooleanOriginal, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "picsrcvd", EbDbTypes.BooleanOriginal, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "dprefid", EbDbTypes.Int32, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "sex", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "district", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "leadowner", EbDbTypes.Int32, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "cust_category", EbDbTypes.Int32, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "baldnessgrade", EbDbTypes.Int32, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "diffusepattern", EbDbTypes.BooleanOriginal, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "hfcurrently", EbDbTypes.BooleanOriginal, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "htpreviously", EbDbTypes.BooleanOriginal, parameters, ref cols, ref vals, ref upcolsvals);
            GetParameter(dict, "country_code", EbDbTypes.String, parameters, ref cols, ref vals, ref upcolsvals);
            //------------------------------------------------------
            GetParameter(dict, "consdate", EbDbTypes.Date, parameters2, ref cols2, ref vals2, ref upcolsvals2);
            GetParameter(dict, "consultingdoctor", EbDbTypes.Int32, parameters2, ref cols2, ref vals2, ref upcolsvals2);
            GetParameter(dict, "noofgrafts", EbDbTypes.Int32, parameters2, ref cols2, ref vals2, ref upcolsvals2);
            GetParameter(dict, "totalrate", EbDbTypes.Int32, parameters2, ref cols2, ref vals2, ref upcolsvals2);
            GetParameter(dict, "prpsessions", EbDbTypes.Int32, parameters2, ref cols2, ref vals2, ref upcolsvals2);
            GetParameter(dict, "consultingfeepaid", EbDbTypes.BooleanOriginal, parameters2, ref cols2, ref vals2, ref upcolsvals2);
            GetParameter(dict, "eb_closing", EbDbTypes.Int32, parameters2, ref cols2, ref vals2, ref upcolsvals2);
            GetParameter(dict, "nature", EbDbTypes.String, parameters2, ref cols2, ref vals2, ref upcolsvals2);
            GetParameter(dict, "probmonth", EbDbTypes.Date, parameters2, ref cols2, ref vals2, ref upcolsvals2);

            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("prehead", EbDbTypes.Int32, 50));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("accountcode", EbDbTypes.String, Fields.Find(i => i.Key == "genurl").Value));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createdby", EbDbTypes.Int32, request.UserId));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_modifiedby", EbDbTypes.Int32, request.UserId));

            parameters2.Add(this.EbConnectionFactory.DataDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName));
            parameters2.Add(this.EbConnectionFactory.DataDB.GetNewParameter("modifiedby", EbDbTypes.String, request.UserName));
            parameters2.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createdby", EbDbTypes.Int32, request.UserId));
            parameters2.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_modifiedby", EbDbTypes.Int32, request.UserId));

            int accid = 0;
            int rstatus = 0;
            if (request.RequestMode == 0)//New Customer
            {
                string Qry = @"INSERT INTO customers(" + cols + @"accountcode, prehead, eb_createdby, eb_createdat, eb_modifiedby, eb_modifiedat) 
										VALUES(" + vals + @":accountcode, :prehead, :eb_createdby, NOW(), :eb_modifiedby, NOW())
										 RETURNING id;";
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(Qry, parameters.ToArray());
                accid = Convert.ToInt32(dt.Rows[0][0]);

                string Qry2 = @"INSERT INTO leadratedetails(" + cols2 + @"customers_id, accountcode, createdby, createddt, eb_createdby, eb_createdat, eb_modifiedby, eb_modifiedat)
										VALUES (" + vals2 + @":accountid, :accountcode, :createdby, NOW(), :eb_createdby, NOW(), :eb_modifiedby, NOW());";
                parameters2.Add(this.EbConnectionFactory.DataDB.GetNewParameter("accountid", EbDbTypes.Int32, accid));
                parameters2.Add(this.EbConnectionFactory.DataDB.GetNewParameter("accountcode", EbDbTypes.String, Fields.Find(i => i.Key == "genurl").Value));
                rstatus = this.EbConnectionFactory.DataDB.InsertTable(Qry2, parameters2.ToArray());
            }
            else if (request.RequestMode == 1)
            {
                List<DbParameter> tempParam = new List<DbParameter>();
                if (dict.TryGetValue("accountid", out found))
                {
                    parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
                    parameters2.Add(this.EbConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
                    tempParam.Add(this.EbConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
                    accid = Convert.ToInt32(found.Value);
                }
                parameters2.Add(this.EbConnectionFactory.DataDB.GetNewParameter("prehead", EbDbTypes.Int32, 50));

                string Qry = @"UPDATE customers SET " + upcolsvals + " eb_modifiedby=:eb_modifiedby, eb_modifiedat=NOW() WHERE prehead = :prehead AND id = :accountid;";
                rstatus = this.EbConnectionFactory.DataDB.UpdateTable(Qry, parameters.ToArray());

                if (rstatus > 0)
                {
                    EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery("SELECT id FROM leadratedetails WHERE customers_id = :accountid;", tempParam.ToArray());
                    if (dt.Rows.Count > 0)
                    {
                        string Qry2 = @"UPDATE leadratedetails SET " + upcolsvals2 + "modifiedby=:modifiedby, modifieddt=NOW(), eb_modifiedby=:eb_modifiedby, eb_modifiedat=NOW() WHERE customers_id = :accountid;";
                        rstatus += this.EbConnectionFactory.DataDB.UpdateTable(Qry2, parameters2.ToArray()) * 10;
                    }
                    else
                    {
                        string Qry2 = @"INSERT INTO leadratedetails(" + cols2 + @"customers_id, accountcode, createdby, createddt, eb_createdby, eb_createdat, eb_modifiedby, eb_modifiedat)
										VALUES (" + vals2 + @":accountid, :accountcode, :createdby, NOW(), :eb_createdby, NOW(), :eb_modifiedby, NOW());";
                        parameters2.Add(this.EbConnectionFactory.DataDB.GetNewParameter("accountid", EbDbTypes.Int32, accid));
                        parameters2.Add(this.EbConnectionFactory.DataDB.GetNewParameter("accountcode", EbDbTypes.String, Fields.Find(i => i.Key == "genurl").Value));
                        rstatus += this.EbConnectionFactory.DataDB.InsertTable(Qry2, parameters2.ToArray()) * 10;
                    }
                }
            }
            rstatus += Update_Table_Customer_Files(accid, request.ImgRefId, request.UserId, dict) * 100;
            Task.Run(() => UpdateIndexedData(this.EbConnectionFactory.DataDB, dict, accid, request.UserId));

            return new SaveCustomerResponse { Status = (request.RequestMode == 0) ? accid : rstatus };
        }

        private void AddData(Dictionary<string, string> SearchData, Dictionary<string, KeyValueType_Field> dict, string label, string key)
        {
            if (dict.ContainsKey(key))
            {
                string _val = Convert.ToString(dict[key].Value);
                if (!string.IsNullOrEmpty(_val))
                    SearchData.Add(label, _val);
            }
        }

        private void UpdateIndexedData(IDatabase DataDB, Dictionary<string, KeyValueType_Field> dict, int DataId, int UserId)
        {
            Dictionary<string, string> SearchData = new Dictionary<string, string>();
            AddData(SearchData, dict, "Name", "name");
            AddData(SearchData, dict, "Mobile", "genurl");
            AddData(SearchData, dict, "Phone", "genphoffice");
            AddData(SearchData, dict, "WhatsApp", "watsapp_phno");
            if (SearchData.Count > 0)
            {
                string JsonData = JsonConvert.SerializeObject(SearchData);
                SearchHelper.InsertOrUpdate_LM(DataDB, JsonData, DataId, UserId);
            }
            else
            {
                SearchHelper.Delete_LM(DataDB, DataId);
            }
        }

        private int Update_Table_Customer_Files(int accountid, string imagerefid, int userid, Dictionary<string, KeyValueType_Field> dict)
        {
            int rstatus = 0;
            Dictionary<string, List<int>> ImgRefId = JsonConvert.DeserializeObject<Dictionary<string, List<int>>>(imagerefid);            
            List<DbParameter> _param = new List<DbParameter>() { EbConnectionFactory.DataDB.GetNewParameter("customer_id", EbDbTypes.Int32, accountid) };
            string st = string.Empty;
            if (dict.TryGetValue("genurl", out KeyValueType_Field val))
            {
                _param.Add(EbConnectionFactory.DataDB.GetNewParameter("genurl", EbDbTypes.String, Convert.ToString(val.Value)));
                st = "OR B.context_sec = 'CustomerPhNo:' || :genurl";
            }

            string selQry = $@"
				SELECT B.id
				FROM eb_files_ref B LEFT JOIN customer_files A 
				ON A.eb_files_ref_id = B.id				
				WHERE ((A.customer_id = @customer_id AND COALESCE(A.eb_del, false) = false) OR B.context_sec = 'CustomerId:{accountid}' OR B.context_sec = 'Prp_CustomerId:{accountid}' {st})
				AND COALESCE(B.eb_del, 'F') = 'F';";

            EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(selQry, _param.ToArray());
            List<int> oldIdsAll = new List<int>();
            foreach (EbDataRow dr in dt.Rows)
                oldIdsAll.Add(Convert.ToInt32(dr[0]));

            IEnumerable<int> delIds = oldIdsAll.Intersect(ImgRefId["attachImg_del"].Concat(ImgRefId["prpImg_del"]));
            IEnumerable<int> insIdsAll = ImgRefId["attachImg_add"].Concat(ImgRefId["prpImg_add"]).Except(oldIdsAll);
            IEnumerable<int> insIdsPrp = ImgRefId["prpImg_add"].Except(oldIdsAll);

            string updateQry = string.Empty;
            List<DbParameter> parameters = new List<DbParameter>();
            if (delIds.Count() > 0)
            {
                updateQry = $@"
					UPDATE customer_files SET eb_del = true 
					WHERE 
						customer_id = @customer_id AND 
						COALESCE(eb_del, false) = false AND 
						eb_files_ref_id = ANY(STRING_TO_ARRAY(@del_ids, ',')::INT[]);

					UPDATE eb_files_ref SET eb_del = 'T', lastmodifiedby = @eb_user_id, lastmodifiedat = {EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP}
					WHERE id = ANY(STRING_TO_ARRAY(@del_ids, ',')::INT[]) AND COALESCE(eb_del, 'F') = 'F';";//B.context_sec = 'CustomerId:{request.CustomerId}

                parameters.Add(EbConnectionFactory.DataDB.GetNewParameter("del_ids", EbDbTypes.String, delIds.Join(",")));
            }
            if (insIdsAll.Count() > 0)
            {
                updateQry += @"INSERT INTO customer_files(customer_id, eb_files_ref_id)
								SELECT @customer_id, ref_id FROM UNNEST(STRING_TO_ARRAY(@ins_ids, ',')::INT[]) AS ref_id;";
                parameters.Add(EbConnectionFactory.DataDB.GetNewParameter("ins_ids", EbDbTypes.String, insIdsAll.Join(",")));
            }
            if (insIdsPrp.Count() > 0)
            {
                updateQry += $@"UPDATE eb_files_ref SET context = 'prp', lastmodifiedby = @eb_user_id, lastmodifiedat = {EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP}
								WHERE id = ANY(STRING_TO_ARRAY(@prp_ids, ',')::INT[]) AND COALESCE(eb_del, 'F') = 'F' AND userid = @eb_user_id;";
                parameters.Add(EbConnectionFactory.DataDB.GetNewParameter("prp_ids", EbDbTypes.String, insIdsPrp.Join(",")));
            }

            if (updateQry != string.Empty)
            {
                parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("customer_id", EbDbTypes.Int32, accountid));
                parameters.Add(EbConnectionFactory.DataDB.GetNewParameter("eb_user_id", EbDbTypes.Int32, userid));
                rstatus = EbConnectionFactory.DataDB.DoNonQuery(updateQry, parameters.ToArray());
            }
            return rstatus;
        }

        public SaveCustomerFollowupResponse Any(SaveCustomerFollowupRequest request)
        {
            int rstatus = 0;
            FeedbackEntry F_Obj = JsonConvert.DeserializeObject<FeedbackEntry>(request.Data);
            List<DbParameter> parameters = new List<DbParameter>();
            //var CrntDateTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("India Standard Time"));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, F_Obj.Id));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("accountid", EbDbTypes.Int32, Convert.ToInt32(F_Obj.Account_Code)));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("trdate", EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(F_Obj.Date, "dd-MM-yyyy", CultureInfo.InvariantCulture))));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("status", EbDbTypes.String, F_Obj.Status));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("followupdate", EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(F_Obj.Fup_Date, "dd-MM-yyyy", CultureInfo.InvariantCulture))));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("narration", EbDbTypes.String, F_Obj.Comments));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName));
            //parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("createddt", EbDbTypes.DateTime, CrntDateTime));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("modifiedby", EbDbTypes.String, request.UserName));
            //parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("modifieddt", EbDbTypes.DateTime, CrntDateTime));

            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createdby", EbDbTypes.Int32, request.UserId));
            //parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createddt", EbDbTypes.DateTime, CrntDateTime));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("isnotpickedup", EbDbTypes.BooleanOriginal, F_Obj.Is_Picked_Up.Equals("Yes") ? false : true));

            if (F_Obj.Id == 0)//new   //if (true)//update disabled
            {
                string Qry = @"INSERT INTO leaddetails(prehead, customers_id, trdate, status, followupdate, narration, createdby, createddt, eb_createdby, eb_createddt, isnotpickedup) 
									VALUES('50' , :accountid, :trdate, :status, :followupdate, :narration, :createdby, NOW(), :eb_createdby, NOW(), :isnotpickedup);";
                rstatus = this.EbConnectionFactory.DataDB.InsertTable(Qry, parameters.ToArray());
            }
            else if (request.Permission)//update
            {
                string Qry = @"UPDATE leaddetails 
								SET trdate=:trdate, status=:status, followupdate=:followupdate, narration=:narration, modifiedby = :modifiedby, modifieddt = NOW(), isnotpickedup = :isnotpickedup
								WHERE prehead = '50' AND customers_id = :accountid AND id=:id;";
                rstatus = this.EbConnectionFactory.DataDB.UpdateTable(Qry, parameters.ToArray());
            }
            return new SaveCustomerFollowupResponse { Status = rstatus };
        }

        public SaveCustomerPaymentResponse Any(SaveCustomerPaymentRequest request)
        {
            int rstatus = 0;
            BillingEntry B_Obj = JsonConvert.DeserializeObject<BillingEntry>(request.Data);
            List<DbParameter> parameters = new List<DbParameter>();
            //var CrntDateTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("India Standard Time"));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, B_Obj.Id));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("accountid", EbDbTypes.Int32, B_Obj.Account_Code));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("trdate", EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(B_Obj.Date, "dd-MM-yyyy", CultureInfo.InvariantCulture))));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("totalamount", EbDbTypes.Int32, B_Obj.Total_Amount));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("advanceamount", EbDbTypes.Int32, B_Obj.Total_Received));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("paymentmode", EbDbTypes.String, B_Obj.Payment_Mode));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("bank", EbDbTypes.String, B_Obj.Bank));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("pdc", EbDbTypes.BooleanOriginal, B_Obj.PDC));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("balanceamount", EbDbTypes.Int32, B_Obj.Balance_Amount));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("cashreceived", EbDbTypes.Int32, B_Obj.Cash_Paid));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName));
            //parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("createddt", EbDbTypes.DateTime, CrntDateTime));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("narration", EbDbTypes.String, B_Obj.Narration));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("modifiedby", EbDbTypes.String, request.UserName));
            //parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("modifieddt", EbDbTypes.DateTime, CrntDateTime));

            //parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createdby", EbDbTypes.Int32, request.UserId));
            //parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createddt", EbDbTypes.DateTime, CrntDateTime));
            //, eb_createdby, eb_createddt
            //, :eb_createdby, :eb_createddt

            if (true)//update disabled  //if (B_Obj.Id == 0)//new
            {
                string Qry = @"INSERT INTO leadpaymentdetails(prehead,customers_id,trdate,totalamount,advanceamount,paymentmode,bank,balanceamount,cashreceived,createdby,createddt,narration,pdc) 
									VALUES (50,:accountid,:trdate,:totalamount,:advanceamount,:paymentmode,:bank,:balanceamount,:cashreceived,:createdby,NOW(),:narration,:pdc);";
                rstatus = this.EbConnectionFactory.DataDB.InsertTable(Qry, parameters.ToArray());
            }
            else//update
            {
                string Qry = @"UPDATE leadpaymentdetails 
								SET paymentmode = :paymentmode, bank = :bank, cashreceived = :cashreceived,
									narration = :narration, modifiedby = :modifiedby, modifieddt = :modifieddt 
								WHERE customers_id=:accountid AND id = :id;";
                rstatus = this.EbConnectionFactory.DataDB.UpdateTable(Qry, parameters.ToArray());
            }
            return new SaveCustomerPaymentResponse { Status = rstatus };
        }

        public SaveSurgeryDetailsResponse Any(SaveSurgeryDetailsRequest request)
        {
            int rstatus = 0;
            SurgeryEntry S_Obj = JsonConvert.DeserializeObject<SurgeryEntry>(request.Data);
            DbParameter[] parameters1 = {
                this.EbConnectionFactory.DataDB.GetNewParameter("customers_id", EbDbTypes.Int32, S_Obj.Account_Code),
                this.EbConnectionFactory.DataDB.GetNewParameter("dateofsurgery", EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(S_Obj.Date, "dd-MM-yyyy", CultureInfo.InvariantCulture))),
                this.EbConnectionFactory.DataDB.GetNewParameter("eb_loc_id", EbDbTypes.Int32, S_Obj.Branch),
                this.EbConnectionFactory.DataDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName)
                //this.EbConnectionFactory.DataDB.GetNewParameter("modifiedby", EbDbTypes.String, request.UserName),
                //this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, S_Obj.Id )
            };

            List<DbParameter> parameters = new List<DbParameter>();
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("customers_id", EbDbTypes.Int32, S_Obj.Account_Code));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("dateofsurgery", EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(S_Obj.Date, "dd-MM-yyyy", CultureInfo.InvariantCulture))));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_loc_id", EbDbTypes.Int32, S_Obj.Branch));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("modifiedby", EbDbTypes.String, request.UserName));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("extractiondone_by", EbDbTypes.Int32, S_Obj.Extract_By));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("implantation_by", EbDbTypes.Int32, S_Obj.Implant_By));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("consent_by", EbDbTypes.Int32, S_Obj.Consent_By));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("anaesthesia_by", EbDbTypes.Int32, S_Obj.Anaesthesia_By));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("post_briefing_by", EbDbTypes.Int32, S_Obj.Post_Brief_By));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("nurses", EbDbTypes.Int32, S_Obj.Nurse));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("complimentary", EbDbTypes.String, S_Obj.Complimentary));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("method", EbDbTypes.String, S_Obj.Method));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("narration", EbDbTypes.String, S_Obj.Comment));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, S_Obj.Id));

            if (S_Obj.Id == 0)//new
            {
                string Qry1 = @"INSERT INTO 
									leadsurgerydetails(customers_id, dateofsurgery, eb_loc_id, createddt, createdby)
								VALUES
									(:customers_id, :dateofsurgery, :eb_loc_id, NOW(), :createdby);";
                this.EbConnectionFactory.DataDB.InsertTable(Qry1, parameters1);
                string Qry = @"INSERT INTO
 									leadsurgerystaffdetails(customers_id, dateofsurgery, eb_loc_id, createddt, createdby, extractiondone_by,
									implantation_by, consent_by, anaesthesia_by, post_briefing_by, nurses_id, complementry, method, narration)
								VALUES
									(:customers_id, :dateofsurgery, :eb_loc_id, NOW(), :createdby, :extractiondone_by,
									:implantation_by, :consent_by, :anaesthesia_by, :post_briefing_by, :nurses, :complimentary, :method, :narration);";
                rstatus = this.EbConnectionFactory.DataDB.InsertTable(Qry, parameters.ToArray());
            }
            else if (request.Permission)//update
            {
                //string Qry1 = @"UPDATE leadsurgerydetails
                //                SET dateofsurgery = :dateofsurgery, eb_loc_id = :eb_loc_id,
                //                    modifiedby = :modifiedby, modifieddt = NOW()
                //                WHERE id = :id AND customers_id=:customers_id;";
                //this.EbConnectionFactory.DataDB.UpdateTable(Qry1, parameters1);

                string Qry = @"UPDATE leadsurgerystaffdetails 
								SET dateofsurgery = :dateofsurgery, eb_loc_id = :eb_loc_id, extractiondone_by = :extractiondone_by,
                                    implantation_by = :implantation_by, consent_by = :consent_by, anaesthesia_by = :anaesthesia_by,
									post_briefing_by = :post_briefing_by, nurses_id = :nurses, complementry = :complimentary,
                                    method = :method, narration = :narration,
                                    modifiedby = :modifiedby, modifieddt = NOW() 
								WHERE id = :id AND customers_id=:customers_id;";
                rstatus = this.EbConnectionFactory.DataDB.UpdateTable(Qry, parameters.ToArray());
            }
            return new SaveSurgeryDetailsResponse { Status = rstatus };
        }

        public LmUniqueCheckResponse Any(LmUniqueCheckRequest request)
        {
            bool rstatus = false;
            DbParameter[] parameters = new DbParameter[]
            {
                this.EbConnectionFactory.DataDB.GetNewParameter("value", EbDbTypes.String, request.Value.Trim())
            };
            EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery("SELECT id FROM customers WHERE genurl = :value OR genphoffice = :value OR watsapp_phno = :value;", parameters);
            if (dt.Rows.Count == 0)
                rstatus = true;
            return new LmUniqueCheckResponse { Status = rstatus };
        }

        //        public LmDeleteImageResponse Any(LmDeleteImageRequest request)
        //        {
        //            string query = @"
        //UPDATE customer_files SET eb_del = true 
        //WHERE 
        //    customer_id = :customer_id AND 
        //    eb_del = false AND 
        //    eb_files_ref_id = ANY(STRING_TO_ARRAY(:ids, ',')::INT[]);

        //UPDATE eb_files_ref SET eb_del = 'T'
        //WHERE id = ANY(STRING_TO_ARRAY(:ids, ',')::INT[]) AND COALESCE(eb_del, 'F') = 'F';";//B.context_sec = 'CustomerId:{request.CustomerId}

        //			int[] refIds = JsonConvert.DeserializeObject<int[]>(request.ImgRefIds);
        //            DbParameter[] parameters = new DbParameter[] {
        //                this.EbConnectionFactory.DataDB.GetNewParameter("customer_id", EbDbTypes.Int32, request.CustId),
        //                this.EbConnectionFactory.DataDB.GetNewParameter("ids", EbDbTypes.String, refIds.Join(","))
        //            };

        //            int rstatus = this.EbConnectionFactory.DataDB.DoNonQuery(query, parameters);

        //            return new LmDeleteImageResponse { RowsAffected = rstatus};
        //        }

        public LmDeleteCustomerResponse Any(LmDeleteCustomerRequest request)
        {
            string query = @"
UPDATE customers SET eb_del = 'T', eb_modifiedby = @modifiedby, eb_modifiedat = NOW()
WHERE id = @id AND COALESCE(eb_del, 'F') = 'F';

UPDATE leaddetails SET eb_del = 'T', modifiedby = @modifiedby, modifieddt = NOW() 
WHERE customers_id = @id AND COALESCE(eb_del, 'F') = 'F';

UPDATE leadratedetails SET eb_del = 'T', modifiedby = @modifiedby, modifieddt = NOW()
WHERE customers_id = @id AND COALESCE(eb_del, 'F') = 'F';

UPDATE leadsurgerydetails SET eb_del = 'T', modifiedby = @modifiedby, modifieddt = NOW() 
WHERE customers_id = @id AND COALESCE(eb_del, 'F') = 'F';

UPDATE leadsurgerystaffdetails SET eb_del = 'T', modifiedby = @modifiedby, modifieddt = NOW()
WHERE customers_id = @id AND COALESCE(eb_del, 'F') = 'F';";

            DbParameter[] parameters = new DbParameter[] {
                this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.CustId),
                this.EbConnectionFactory.DataDB.GetNewParameter("modifiedby", EbDbTypes.Int32, request.UserId)
            };

            int rstatus = this.EbConnectionFactory.DataDB.UpdateTable(query, parameters);

            Task.Run(() => SearchHelper.Delete_LM(this.EbConnectionFactory.DataDB, request.CustId));

            return new LmDeleteCustomerResponse { Status = rstatus > 0 };
        }


        //private int InsertToTable(string TblName, List<KeyValueType_Field> Fields)
        //{
        //	List<DbParameter> parameters = new List<DbParameter>();
        //	string cols = string.Empty;
        //	string vals = string.Empty;
        //	foreach (KeyValueType_Field item in Fields)
        //	{
        //		parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter(item.Key, item.Type, item.Value));
        //		cols += item.Key + ",";
        //		vals += ":" + item.Key + ",";
        //	}
        //	string strQry = @"INSERT INTO @tblname@(@cols@) VALUES(@vals@);"
        //						.Replace("@tblname@", TblName)
        //						.Replace("@cols@", cols.Substring(0, cols.Length - 1))
        //						.Replace("@vals@", vals.Substring(0, vals.Length - 1));
        //	this.EbConnectionFactory.DataDB.InsertTable(strQry, parameters.ToArray());
        //	return 1;
        //}

        //private int UpdateToTable(string TblName, List<KeyValueType_Field> Fields, List<KeyValueType_Field> WhereFields)
        //{
        //	List<DbParameter> parameters = new List<DbParameter>();
        //	string s_set = string.Empty;
        //	string s_where = string.Empty;
        //	foreach (KeyValueType_Field item in Fields)
        //	{
        //		parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter(item.Key, item.Type, item.Value));
        //		s_set += item.Key + "=:" + item.Key + ",";
        //	}
        //	foreach (KeyValueType_Field item in WhereFields)
        //	{
        //		parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter(item.Key, item.Type, item.Value));
        //		s_where += item.Key + "=:" + item.Key + " AND ";
        //	}
        //	string strQry = @"UPDATE @tblname@ SET @str_set@ WHERE @str_where@;"
        //						.Replace("@tblname@", TblName)
        //						.Replace("@str_set@", s_set.Substring(0, s_set.Length - 1))
        //						.Replace("@str_where@", s_where.Substring(0, s_where.Length - 4));
        //	this.EbConnectionFactory.DataDB.UpdateTable(strQry, parameters.ToArray());
        //	return 1;
        //}
    }
}
