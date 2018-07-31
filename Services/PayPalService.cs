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

namespace ExpressBase.ServiceStack.Services
{
    public class PayPalService : EbBaseService
    {
        string OAuthResponse;
        int OAuthStatusCode = 0;
        string P_PayResponse;
        int P_PayResCode;
        string AcceptUrl = string.Empty;
        string ExecuteUrl = string.Empty;
        string CancelUrl = "https://payment.eb-test.info/paymentreturn/paypalreturn?res=cancel";
        string ReturnUrl = "https://payment.eb-test.info/paymentreturn/paypalreturn?res=accept&tok=";

        private PayPalOauthObject _payPalOauth = new PayPalOauthObject();

        FlurlClient flurlClient = new FlurlClient(PayPalConstants.UriString);

        private PayPalOauthObject MakeNewOAuth()
        {
            PayPalOauthObject OAuth = new PayPalOauthObject();
            string UserID = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_PAYPAL_USERID);
            string UserSecret = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_PAYPAL_USERSECRET);
            var client = new RestClient(new Uri(PayPalConstants.UriString));
            System.Net.CookieContainer CookieJar = new System.Net.CookieContainer();
            client.Authenticator = new HttpBasicAuthenticator(UserID, UserSecret);
            client.CookieContainer = CookieJar;

            var OAuthRequest = new RestRequest("v1/oauth2/token", Method.POST);
            OAuthRequest.AddHeader("Accept", "application/json");
            OAuthRequest.AddHeader("Accept-Language", "en_US");
            OAuthRequest.AddHeader("Content-Type", "application/x-www-form-urlencoded");
            OAuthRequest.AddParameter("grant_type", "client_credentials");
            var res = client.ExecuteAsyncPost(OAuthRequest, PayPalCallback, "POST");

            int _timeout = 0;
            while (OAuthStatusCode == 0)
            {
                if (_timeout >= 60000)
                    break;
                System.Threading.Thread.Sleep(500);
                _timeout += 500;
            }

            Console.WriteLine("OAUTH REQUEST\n***************");
            Console.WriteLine("HTTP Response Status Code :: " + OAuthStatusCode.ToString());
            Console.WriteLine("Message Body :: " + OAuthResponse);
            var StreamData = GenerateStreamFromString(OAuthResponse);
            var serializer = new DataContractJsonSerializer(typeof(PayPalOauthObject));
            OAuth = serializer.ReadObject(StreamData) as PayPalOauthObject;

            Console.WriteLine("\nNONCE:: " + OAuth.Nonce +
                "\nAccess Token:: " + OAuth.AccessToken +
                "\nToken Type:: " + OAuth.TokenType +
                "\nApp ID:: " + OAuth.AppId +
                "\nExpires In:: " + OAuth.ExpiresIn.ToString());
            OAuth.SetExpireTime();
            return OAuth;
        }

        PayPalOauthObject PayPalOauth
        {
            get
            {

                if (_payPalOauth.AccessToken == string.Empty)
                {
                    _payPalOauth = this.Redis.Get<PayPalOauthObject>("EB_PAYPAL_OAUTH");
                }

                if (_payPalOauth.GetExpireTime() < DateTime.Now)
                {
                    _payPalOauth = MakeNewOAuth();
                    this.Redis.Set<PayPalOauthObject>("EB_PAYPAL_OAUTH", _payPalOauth);
                }
                return _payPalOauth;
            }
        }

        public PayPalService(IEbConnectionFactory _dbf) : base(_dbf)
        {
            flurlClient.Headers.Add("Content-Type", "application/json");
            flurlClient.Headers.Add("Authorization", "Bearer " + PayPalOauth.AccessToken);
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
            if (response.StatusCode == HttpStatusCode.OK)
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

        BillingPlanResponse CreateBillingPlan(PayPalOauthObject payPalOauth, FlurlClient flurlClient, int _users, double _amount)
        {
            double PaymentSum = _users * _amount;
            double TaxPercent = 0.3;
            BillingPlanRequest Plan = new BillingPlanRequest();
            Plan.Name = "Test Plan";
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
            
            //need to change the below line - OAuth tokens are NOT supposed to be transferred like this
            Plan.MerchantPref.ReturnUrl = ReturnUrl + payPalOauth.AccessToken;
            Plan.MerchantPref.CancelUrl = CancelUrl;
            Plan.MerchantPref.AutoBillAmount = "YES";
            Plan.MerchantPref.InitialFailAmountAction = "CONTINUE";
            Plan.MerchantPref.MaxFailAttempts = "1";


            //PREPARING REQUEST TO CREATE BILLING PLAN
            string JsonBody = JsonConvert.SerializeObject(Plan, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });

            FlurlRequest flurlRequest = new FlurlRequest(PayPalConstants.BillingPlanPath);
            flurlRequest.Client = flurlClient;


            //SENDING BILLING PLAN REQUEST AND RECEIVING RESPONSE
            var PaymentPlanResult = Send(HttpMethod.Post, flurlRequest, JsonBody).Result;
            var ResultContents = GetResponseContents(PaymentPlanResult).Result;
            Console.WriteLine("Payment Plan Create Request :: ");
            Console.WriteLine("Response Code :: " + PaymentPlanResult.StatusCode);
            Console.WriteLine("Response :: " + ResultContents);
            BillingPlanResponse BillPlanResponse = new BillingPlanResponse();
            BillPlanResponse = JsonConvert.DeserializeObject<BillingPlanResponse>(ResultContents);
            Console.WriteLine("\nBilling Plan Response ID :: " + BillPlanResponse.PlanID);
            string PaymentPlanID = BillPlanResponse.PlanID;
            string Url = PayPalConstants.BillingPlanPath + PaymentPlanID + "/";
            FlurlRequest ActivateRequest = new FlurlRequest(Url);
            ActivateRequest.Client = flurlClient;
            string _activateJson = "[{  \"op\": \"replace\",  \"path\": \"/\",  \"value\": {    \"state\": \"ACTIVE\"  }}]";
            var ActResult = Send(new HttpMethod("PATCH"), ActivateRequest, _activateJson).Result;
            var ActResultContents = GetResponseContents(ActResult).Result;
            if (ActResult.StatusCode == HttpStatusCode.OK)
            {
                BillPlanResponse.CurrentState = BillingPlanResponse.PlanStateStrings[(int)PlanState.ACTIVE];
            }
            Console.WriteLine("\n\nPayment Plan Activation Request :: ");
            Console.WriteLine("Response Code :: " + ActResult.StatusCode);
            Console.WriteLine("Response :: " + ActResultContents);

            return BillPlanResponse;

        }

        public string GetBillingAgreementID()
        {
            throw new NotImplementedException();
            //return "";
        }

        public BillingAgreementResponse CreateBillingAgreement(PayPalPaymentRequest req, string BillingPlanID, int UserCount)
        {
            FundingInstrument fundingInstrument = new FundingInstrument();

            BillingAgreementRequest BillAgreementRequest = new BillingAgreementRequest()
            {
                Name = "Billing Request - ID: " + req.TenantAccountId,
                Description = "Billing agreement for client: " + req.TenantAccountId + " Number of users: " + UserCount.ToString() + " Total amount charged: $" + (UserCount * 5).ToString(),
                StartDate = DateTime.Now.AddMinutes(5.0).ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'"),//"2020-01-01T09:13:49Z",
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

        [Authenticate]
        public PayPalPaymentResponse Post(PayPalPaymentRequest req)
        {
            int UserCount = 5;//this.Redis.Get<Eb_Solution>(String.Format("solution_{0}",req.TenantAccountId)).NumberOfUsers;
            PayPalPaymentResponse resp = new PayPalPaymentResponse();

            string ppBillingId = GetBillingPlanId(UserCount, 5);
            string BillingAgreementID = string.Empty;
            var Agreement = CreateBillingAgreement(req, ppBillingId, UserCount);
            foreach (var _link in Agreement.Links)
            {
                if (_link.Rel == "approval_url")
                    AcceptUrl = _link.Href;
                if (_link.Rel == "execute")
                    ExecuteUrl = _link.Href;
            }
            
            resp.Test = AcceptUrl;
            return resp;
        }

        bool PersistBillingPlan(EbBillingPlan plan)
        {

            string sql = @"INSERT INTO eb_pp_billing_plans(pp_billingplan_id, num_users, amount_per_user/*, currency*/) VALUES(@plan_id, @num, @amount/*, @currency*/) RETURNING id";
            DbParameter[] parameters =
            {
                        this.InfraConnectionFactory.DataDB.GetNewParameter("plan_id", EbDbTypes.String, plan.PlanResponse.PlanID),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("num",EbDbTypes.Int16, plan.NumUsers),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("amount",EbDbTypes.Decimal, plan.AmountperUser),
                        //this.InfraConnectionFactory.DataDB.GetNewParameter("currency",EbDbTypes.Int64, plan.PlanResponse.CurrencyCode.ToString()), //To String

            };
            var id = this.EbConnectionFactory.DataDB.DoNonQuery(sql, parameters);

            return ((id > 0) ? true : false);
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
                //CREATING BILLING PLAN && ACTIVATING THE BILLING PLAN
                BillingPlanResponse BillPlanResponse = CreateBillingPlan(PayPalOauth, flurlClient, users, amount);

                EbBillingPlan plan = new EbBillingPlan(10, 5, BillPlanResponse);
                var x = PersistBillingPlan(plan);

                ppBillingId = BillPlanResponse.PlanID;

            }
            return ppBillingId;
        }
    }
}
