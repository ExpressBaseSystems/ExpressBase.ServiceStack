using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Enums;
using ExpressBase.Common.ServiceStack.ReqNRes;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Flurl.Http;
using Newtonsoft.Json;
using RestSharp;
using RestSharp.Extensions;
using RestSharp.Authenticators;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Threading.Tasks;
using ExpressBase.Common.Structures;
using System.Data.Common;
using ExpressBase.Common.LocationNSolution;
using ExpressBase.Common.Constants;
using System.Text;

namespace ExpressBase.ServiceStack.Services
{
    public class PayPalService : EbBaseService
    {
        string OAuthResponse;
        int OAuthStatusCode = 0;
        volatile int UserCount = 0;
        int PricePerUser = 5;
        string Currency = "USD";
        string CancelPage = "/PayPal/CancelAgreement/",
            ReturnPage = "/PayPal/ReturnSuccess/",
            CancelUrl = string.Empty,
            ReturnUrl = string.Empty;

        private PayPalOauthObject _payPalOAuth = null;
        PayPalPaymentResponse PayPalResponse;

        FlurlClient flurlClient = new FlurlClient(PayPalConstants.UriString);

        private PayPalOauthObject MakeNewOAuth()
        {
            string UserID = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_PAYPAL_USERID),
            UserSecret = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_PAYPAL_USERSECRET);
            var client = new RestClient(new Uri(PayPalConstants.UriString));
            CookieContainer CookieJar = new CookieContainer();
            client.Authenticator = new HttpBasicAuthenticator(UserID, UserSecret);
            client.CookieContainer = CookieJar;

            var OAuthRequest = new RestRequest("v1/oauth2/token", Method.POST);
            OAuthRequest.AddHeader("Accept", "application/json");
            OAuthRequest.AddHeader("Accept-Language", "en_US");
            OAuthRequest.AddHeader("Content-Type", "application/x-www-form-urlencoded");
            OAuthRequest.AddParameter("grant_type", "client_credentials");
            client.ExecuteAsyncPost(OAuthRequest, PayPalCallback, "POST");

            int _timeout = 0;
            while (OAuthStatusCode.Equals(0))
            {
                if (_timeout >= 60000)
                    break;
                System.Threading.Thread.Sleep(500);
                _timeout += 500;
            }

            var StreamData = GenerateStreamFromString(OAuthResponse);
            PayPalOauthObject OAuth = new DataContractJsonSerializer(typeof(PayPalOauthObject)).ReadObject(StreamData) as PayPalOauthObject;
            this.Redis.Set<PayPalOauthObject>("EB_PAYPAL_OAUTH", OAuth);
            return OAuth;
        }

        PayPalOauthObject PayPalOauth
        {
            get
            {

                if (_payPalOAuth == null)
                {
                    _payPalOAuth = this.Redis.Get<PayPalOauthObject>("EB_PAYPAL_OAUTH");

                    if (_payPalOAuth == null)
                        _payPalOAuth = MakeNewOAuth();
                }
                if (_payPalOAuth.ExpireTime < DateTime.Now)
                    _payPalOAuth = MakeNewOAuth();

                return _payPalOAuth;
            }
        }

        public PayPalService(IEbConnectionFactory _dbf) : base(_dbf)
        {
            flurlClient.Headers.Add("Content-Type", "application/json");
            flurlClient.Headers.Add("Authorization", "Bearer " + PayPalOauth.Token);
        }

        public System.IO.Stream GenerateStreamFromString(string s)
        {
            var stream = new System.IO.MemoryStream();
            var writer = new System.IO.StreamWriter(stream);
            writer.Write(s);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }

        async Task<string> GetResponseContents(HttpResponseMessage Response)
        {
            return await Response.Content.ReadAsStringAsync();
        }

        void PayPalCallback(IRestResponse response, RestRequestAsyncHandle handle)
        {
            if (response.StatusCode.Equals(HttpStatusCode.OK))
            {
                OAuthResponse = response.Content;
                OAuthStatusCode = (int)response.StatusCode;
            }
            else
            {
                OAuthStatusCode = (int)response.StatusCode;
                Console.WriteLine("\nRequest Failed");
            }
        }

        async Task<HttpResponseMessage> Send(HttpMethod _method, Flurl.Http.FlurlRequest flurlRequest, string JsonBody)
        {
            return await flurlRequest.SendAsync(_method, new Flurl.Http.Content.CapturedJsonContent(JsonBody));
        }

        BillingPlanResponse CreateBillingPlan(PayPalOauthObject payPalOauth, FlurlClient flurlClient)
        {
            double PaymentSum = UserCount * PricePerUser;
            double TaxPercent = 0.3;
            BillingPlanRequest Plan = new BillingPlanRequest();
            Plan.Name = "Billing Plan - Price: " + PaymentSum + " " + Currency;
            Plan.Description = "Testing the Billing Plan in PayPal";
            Plan.Type = "FIXED";
            Plan.PaymentDef = new List<PaymentDefinition>();


            //PLAN DEFINITION - HARD CODED FOR NOW, NEEDS TO BE CREATED DYNAMICALLY IN PRODUCTION
            Plan.PaymentDef.Add(
            new PaymentDefinition()
            {
                Name = "Regular payment definition",
                PaymentType = "REGULAR",
                Frequency = "MONTH",
                FrequencyInterval = "1",
                Amount = new Dictionary<string, string>() { { "value", PaymentSum.ToString() }, { "currency", "USD" } },
                Cycles = "12",
                ChargeModels = new List<ChargeModel>()
                {
                    new ChargeModel()
                    {
                        ChargeType = "TAX",
                        ChargeAmount = new Dictionary<string, string>(){{ "value", (PaymentSum*TaxPercent).ToString() }, { "currency", "USD" } }
                    },
                }
            });

            Plan.MerchantPref.SetupFee = null;// new Dictionary<string, string>() { { "value", "1" }, { "currency", "USD" } };

            Plan.MerchantPref.ReturnUrl = ReturnUrl;
            Plan.MerchantPref.CancelUrl = CancelUrl;
            Plan.MerchantPref.AutoBillAmount = "YES";
            Plan.MerchantPref.InitialFailAmountAction = "CONTINUE";
            Plan.MerchantPref.MaxFailAttempts = "1";

            string JsonBody = JsonConvert.SerializeObject(Plan, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });

            FlurlRequest flurlRequest = new FlurlRequest(PayPalConstants.BillingPlanPath);
            flurlRequest.Client = flurlClient;
            var PaymentPlanResult = Send(HttpMethod.Post, flurlRequest, JsonBody).Result;
            var ResultContents = GetResponseContents(PaymentPlanResult).Result;
            Console.WriteLine("Payment Plan Create Request :: ");
            Console.WriteLine("Response Code :: " + PaymentPlanResult.StatusCode);
            Console.WriteLine("Response :: " + ResultContents);
            BillingPlanResponse BillPlanResponse = new BillingPlanResponse();
            BillPlanResponse = JsonConvert.DeserializeObject<BillingPlanResponse>(ResultContents);
            Console.WriteLine("\nBilling Plan Response ID :: " + BillPlanResponse.PlanID);
            string PaymentPlanID = BillPlanResponse.PlanID;
            if (ActivatePlan(flurlClient, PaymentPlanID))
            {
                BillPlanResponse.CurrentState = BillingPlanResponse.PlanStateStrings[(int)PlanState.ACTIVE];
                Console.WriteLine("DEBUG MESSAGE: Activated Plan Successfully - " + PaymentPlanID);
            }
            else
            {
                Console.WriteLine("DEBUG MESSAGE: Activation Failed for Plan - " + PaymentPlanID);
            }
            return BillPlanResponse;
        }

        private bool ActivatePlan(FlurlClient flurlClient, string PaymentPlanID)
        {
            string Url = PayPalConstants.BillingPlanPath + PaymentPlanID + "/";
            FlurlRequest ActivateRequest = new FlurlRequest(Url);
            ActivateRequest.Client = flurlClient;
            string _activateJson = "[{  \"op\": \"replace\",  \"path\": \"/\",  \"value\": {    \"state\": \"ACTIVE\"  }}]";
            var ActResult = Send(new HttpMethod("PATCH"), ActivateRequest, _activateJson).Result;
            Console.WriteLine("\n\nPayment Plan Activation Request: ");
            Console.WriteLine("Response Code :: " + ActResult.StatusCode);
            return ActResult.StatusCode.Equals(HttpStatusCode.OK);
        }

        public BillingAgreementResponse CreateBillingAgreement(string TenantAccountId, string BillingPlanID)
        {
            double TotalPrice = UserCount * PricePerUser;
            FundingInstrument fundingInstrument = new FundingInstrument();
            string _startDate = DateTime.Now.AddMinutes(5.0).ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'");
            BillingAgreementRequest BillAgreementRequest = new BillingAgreementRequest()
            {
                Name = "Billing Agreement - Solution ID: " + TenantAccountId + " Price: " + TotalPrice + " " + Currency,
                Description = "Billing agreement for client: " + TenantAccountId + " Number of users: " + UserCount.ToString() + " Total amount charged: " 
                + (TotalPrice).ToString() + " " + Currency + " Starting from: " + _startDate,
                StartDate = _startDate,//"2020-01-01T09:13:49Z",
                BillingPlan = new BillingPlanResponse(BillingPlanID),
                Payer = new PayerDetails()
                {
                    PayMethod = PayerDetails.PaymentMethodsStrings[(int)PaymentMethod.paypal],
                    FundingInstruments = null,
                    FundingOptionId = null
                }
            };

            FlurlRequest AgreementRequest = new FlurlRequest(PayPalConstants.AgreementUrl);
            AgreementRequest.Client = flurlClient;
            string AgreementJson = JsonConvert.SerializeObject(BillAgreementRequest, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
            var AgreementResult = Send(HttpMethod.Post, AgreementRequest, AgreementJson).Result;
            var AgreementResConents = GetResponseContents(AgreementResult).Result;
            Console.WriteLine("\n\nBilling Agreement Creating Request :: ");
            Console.WriteLine("Response Code :: " + AgreementResult.StatusCode);
            Console.WriteLine("Response :: " + AgreementResConents);
            return JsonConvert.DeserializeObject<BillingAgreementResponse>(AgreementResConents);
        }

        private string ConstructExecuteUrl(string PaymentId)
        {
            return new StringBuilder()
                .Append(PayPalConstants.AgreementUrl)
                .Append(PaymentId)
                .Append("/agreement-execute").ToString();
        }

        private bool SavePayPalAgreement(BillingAgreementResponse FinalResponse, string ResponseContents, string SolutionId)
        {
            string sql = @"INSERT INTO eb_pp_subscriptions(solution_id, pp_billing_plan_id, agreement_creation_date, is_canceled, start_date, raw_json) VALUES(@solution_id, @billing_plan_id, @agc_date, @canceled, @s_date, @json) RETURNING id";
            
            DbParameter[] parameters =
            {
                        this.InfraConnectionFactory.DataDB.GetNewParameter("solution_id", EbDbTypes.String, SolutionId),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("pp_billing_plan_id",EbDbTypes.Int32, FinalResponse.AgreementPlan.PlanID),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("agreement_creation_date",EbDbTypes.String, DateTime.Now.ToString("dd-MM-yyyy")),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("is_canceled",EbDbTypes.String, (FinalResponse.AgreementState=="Cancelled")?"true":"false"),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("start_date",EbDbTypes.String, FinalResponse.StartDate),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("raw_json",EbDbTypes.String, ResponseContents),

            };
            var id = this.EbConnectionFactory.DataDB.DoNonQuery(sql, parameters);

            return id > 0;
        }

        public void SaveRejectedPayment()
        {

        }

        private bool SavePayPalWebHookJson(string JsonString, int state, string[] ActionComponents)
        {
            string sql = @"INSERT INTO eb_test_webhook_json(json_value, state, action1, action2, action3) VALUES(@json_str, @state, @ac1, @ac2, @ac3) RETURNING id";

            DbParameter[] parameters =
            {
                this.InfraConnectionFactory.DataDB.GetNewParameter("json_str",EbDbTypes.String, JsonString),
                this.InfraConnectionFactory.DataDB.GetNewParameter("state",EbDbTypes.Int32, state),
                this.InfraConnectionFactory.DataDB.GetNewParameter("ac1",EbDbTypes.String, ActionComponents[0]),
                this.InfraConnectionFactory.DataDB.GetNewParameter("ac2",EbDbTypes.String, ActionComponents[1]),
                this.InfraConnectionFactory.DataDB.GetNewParameter("ac3",EbDbTypes.String, ActionComponents[2]),
            };
            var id = this.EbConnectionFactory.DataDB.DoNonQuery(sql, parameters);

            return id > 0;
        }

        [Authenticate]
        public void Post(PayPalWebHookHandler handler)
        {
            int state = 0;
            string[] ActionComponents = handler.Action.Split('.'); ;
            if (handler.JsonBody==string.Empty)
            {
                Console.WriteLine("The JSON body is empty");
            }
            else
            {
                Console.WriteLine("JSON Response: \n" + handler.JsonBody);

                SavePayPalWebHookJson(handler.JsonBody, state, ActionComponents);

                StringBuilder responseBuilder = new StringBuilder();
                if (ActionComponents[0].Equals("billing"))
                    state += 10;

                else if (ActionComponents[0].Equals("checkout"))
                    state += 100;

                else
                    state -= 100;

                if (state < 0)
                {
                    responseBuilder.Append("Invalid Action Component Passed : ");
                    responseBuilder.Append(ActionComponents[0]);
                }
                else
                {
                    if (ActionComponents[1].Equals("plan") && state < 100)
                        state += 1000;
                    else if (ActionComponents[1].Equals("subscription") && state < 100)
                        state += 2000;
                    else
                        state -= 3000;

                    if (ActionComponents[1].Equals("order") && state > 10)
                        state += 3000;
                    else
                        state -= 3500;
                }

                if(state < 0)
                {
                    responseBuilder.Append(" ");
                    responseBuilder.Append(ActionComponents[1]);
                }
                else
                {
                    if (ActionComponents[2].Equals("created"))
                        state += 1;
                    if (ActionComponents[2].Equals("updated"))
                        state += 2;
                    if (ActionComponents[2].Equals("cancelled"))
                        state += 3;
                    if (ActionComponents[2].Equals("reactivated"))
                        state += 4;
                    if (ActionComponents[2].Equals("suspended"))
                        state += 5;
                    if (ActionComponents[2].Equals("completed"))
                        state += 6;

                    if (state > 0 && state % 10 == 0)
                        state -= 4000;

                    if (state <= 0)
                    {
                        Console.WriteLine("Error occurred while decoding Action: " + responseBuilder.ToString());
                        return;
                    }

                    else
                    {
                        if(state.Equals(1011))
                        {
                            Console.WriteLine("Instantiating Handle for Billing Plan Created Event");
                        }
                        else if(state.Equals(1012))
                        {
                            Console.WriteLine("Instantiating Handle for Billing Plan Updated Event");
                        }
                        else if(state.Equals(2011))
                        {
                            Console.WriteLine("Instantiating Handle for Billing Subscription Created Event");
                        }
                        else if(state.Equals(2012))
                        {
                            Console.WriteLine("Instantiating Handle for Billing Subscription Updated Event");
                        }
                        else if(state.Equals(2013))
                        {
                            Console.WriteLine("Instantiating Handle for Billing Subscription Cancelled Event");
                        }
                        else if(state.Equals(2014))
                        {
                            Console.WriteLine("Instantiating Handle for Billing Subscription Reactivated Event");
                        }
                        else if (state.Equals(2015))
                        {
                            Console.WriteLine("Instantiating Handle for Billing Subscription Suspended Event");
                        }
                        else if (state.Equals(3016))
                        {
                            Console.WriteLine("Instantiating Handle for Checkout Order Completed Event");
                        }
                        else
                        {
                            Console.WriteLine("Error: Wrong Code Created. Check state assignment or responses received.");
                        }
                    }

                }
            }
        }

        [Authenticate]
        public void Post(PayPalFailureReturnRequest req)
        {
            
        }


        [Authenticate]
        public void Post(PayPalSuccessReturnRequest req)
        {
            try
            {
                string PayId = req.PaymentId;
                FlurlRequest ExecuteRequest = new FlurlRequest(ConstructExecuteUrl(PayId));
                ExecuteRequest.Client = flurlClient;
                var ExecuteResponse = Send(HttpMethod.Post, ExecuteRequest, "").Result;
                var ResponseContents = GetResponseContents(ExecuteResponse).Result;
                Console.WriteLine("Execute Response Status Code: " + ExecuteResponse.StatusCode);
                Console.WriteLine("Response: " + ResponseContents);
                BillingAgreementResponse FinalResponse = JsonConvert.DeserializeObject<BillingAgreementResponse>(ResponseContents);
                if (!SavePayPalAgreement(FinalResponse, ResponseContents, req.SolutionId))
                    throw new Exception("Failed to save data to the database");
            }
            catch(Exception ex)
            {
                Console.WriteLine("Exception Thrown : " + ex);
            }
        }

        [Authenticate]
        public PayPalPaymentResponse Post(PayPalPaymentRequest req)
        {
            //this.Redis.Get<Eb_Solution>(String.Format("solution_{0}",req.TenantAccountId)).NumberOfUsers;
            PayPalResponse = new PayPalPaymentResponse();
            try
            {
                CancelUrl = req.Environment + CancelPage + req.SolutionId;
                ReturnUrl = req.Environment + ReturnPage + req.SolutionId;
                UserCount = req.UserCount;
                string ppBillingId = GetBillingPlanId(UserCount, 5);//(UserCount, 5);
                string BillingAgreementID = string.Empty;
                var Agreement = CreateBillingAgreement(req.SolutionId, ppBillingId);
                foreach (var _link in Agreement.Links)
                {
                    if (_link.Rel.Equals("approval_url"))
                        PayPalResponse.ApprovalUrl = _link.Href;
                    if (_link.Rel.Equals("execute"))
                        PayPalResponse.ExecuteUrl = _link.Href;
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine("Exception thrown : " + ex);
            }
            return PayPalResponse;
        }

        bool PersistBillingPlan(EbBillingPlan plan)
        {

            string sql = @"INSERT INTO eb_pp_billing_plans(pp_billingplan_id, num_users, amount_per_user, currency) VALUES(@plan_id, @num, @amount, @curr_code) RETURNING id";
            string _currencyCode = string.Empty;
            foreach(var _temp in plan.PlanResponse.PaymentDefinitions)
            {
                if (_temp.PaymentType=="REGULAR")
                    _currencyCode = _temp.Amount["currency"];
            }
            DbParameter[] parameters =
            {
                        this.InfraConnectionFactory.DataDB.GetNewParameter("plan_id", EbDbTypes.String, plan.PlanResponse.PlanID),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("num",EbDbTypes.Int16, plan.NumUsers),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("amount",EbDbTypes.Decimal, plan.AmountperUser),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("curr_code",EbDbTypes.String, _currencyCode), 

            };
            var id = this.EbConnectionFactory.DataDB.DoNonQuery(sql, parameters);

            return id > 0;
        }

        string GetBillingPlanId(int users, double amount)
        {
            string ppBillingId = null;

            string sql = "SELECT pp_billingplan_id FROM eb_pp_billing_plans WHERE num_users=@num AND amount_per_user = @amount"; // Include currency and Merchent ID
            DbParameter[] parameters =
            {
                        this.InfraConnectionFactory.DataDB.GetNewParameter("num", EbDbTypes.Int16, users),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("amount",EbDbTypes.Decimal, amount)
            };

            ppBillingId = this.EbConnectionFactory.DataDB.DoQuery<string>(sql, parameters);

            if (string.IsNullOrEmpty(ppBillingId))
            {
                BillingPlanResponse BillPlanResponse = CreateBillingPlan(PayPalOauth, flurlClient);

                EbBillingPlan plan = new EbBillingPlan(users, amount, BillPlanResponse);
                var x = PersistBillingPlan(plan);

                ppBillingId = BillPlanResponse.PlanID;

            }
            return ppBillingId;
        }
    }
}
