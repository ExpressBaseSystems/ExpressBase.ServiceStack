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

namespace ExpressBase.ServiceStack.Services
{


    public class PayPalService : EbBaseService
    {
        const string OAuthTokenPath = "/v1/oauth2/token";
        const string UriString = "https://api.sandbox.paypal.com/";
        string Response;
        int StatusCode;
        string P_PayResponse;
        int P_PayResCode;

        public PayPalService(IEbConnectionFactory _dbf) : base(_dbf)
        {
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

        public System.IO.Stream GenerateStreamFromObject(object s)
        {
            var stream = new System.IO.MemoryStream();
            var writer = new System.IO.StreamWriter(stream);
            writer.Write(s);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }

        public string GetStringFromStream(System.IO.Stream stream)
        {
            System.IO.StreamReader reader = new System.IO.StreamReader(stream);
            string str = reader.ReadToEnd();
            return str;
        }

        async Task<string> GetResponseContents(HttpResponseMessage Response)
        {
            return await Response.Content.ReadAsStringAsync();
        }

        void PayPalCallback(IRestResponse response, RestRequestAsyncHandle handle)
        {
            if (response.StatusCode == HttpStatusCode.OK)
            {
                Response = response.Content;
                StatusCode = (int)response.StatusCode;
            }
            else
            {
                StatusCode = (int)response.StatusCode;
                Console.WriteLine("\nRequest Failed");
            }
        }

        async Task<HttpResponseMessage> Send(HttpMethod _method, Flurl.Http.FlurlRequest flurlRequest, string JsonBody)
        {
            return await flurlRequest.SendAsync(_method, new Flurl.Http.Content.CapturedJsonContent(JsonBody));
        }

        PayPalOauthObject CreateOAuthToken()
        {
            string UserID = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_PAYPAL_USERID);
            string UserSecret = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_PAYPAL_USERSECRET);
            var client = new RestClient(new Uri(UriString));
            System.Net.CookieContainer CookieJar = new System.Net.CookieContainer();
            client.Authenticator = new HttpBasicAuthenticator(UserID, UserSecret);
            client.CookieContainer = CookieJar;


            //SENDING OAUTH-BASED AUTHENTICATION REQUEST
            var OAuthRequest = new RestRequest("v1/oauth2/token", Method.POST);
            OAuthRequest.AddHeader("Accept", "application/json");
            OAuthRequest.AddHeader("Accept-Language", "en_US");
            OAuthRequest.AddHeader("Content-Type", "application/x-www-form-urlencoded");
            OAuthRequest.AddParameter("grant_type", "client_credentials");


            //RECEIVING AND PARSING OAUTH RESPONSE
            var res = client.ExecuteAsyncPost(OAuthRequest, PayPalCallback, "POST");
            System.Threading.Thread.Sleep(8000);

            Console.WriteLine("OAUTH REQUEST\n***************");
            Console.WriteLine("HTTP Response Status Code :: " + StatusCode.ToString());
            Console.WriteLine("Message Body :: " + Response);
            var StreamData = GenerateStreamFromString(Response);
            var serializer = new DataContractJsonSerializer(typeof(PayPalOauthObject));
            var SerialOauthResponse = serializer.ReadObject(StreamData) as PayPalOauthObject;

            Console.WriteLine("\nNONCE:: " + SerialOauthResponse.Nonce +
                "\nAccess Token:: " + SerialOauthResponse.AccessToken +
                "\nToken Type:: " + SerialOauthResponse.TokenType +
                "\nApp ID:: " + SerialOauthResponse.AppId +
                "\nExpires In:: " + SerialOauthResponse.ExpiresIn.ToString());
            this.Redis.Set<PayPalOauthObject>("EB_PAYPAL_OAUTH", SerialOauthResponse);
            return SerialOauthResponse;
        }

        BillingPlanResponse GetBillingPlan(PayPalOauthObject payPalOauth, FlurlClient flurlClient)
        {
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
                FrequencyInterval = "2",
                Amount = new Dictionary<string, string>() { { "value", "100" }, { "currency", "USD" } },
                Cycles = "12",
                ChargeModels = new List<ChargeModel>()
                {
                    new ChargeModel()
                    {
                        ChargeType = "SHIPPING",
                        ChargeAmount = new Dictionary<string, string>() {{ "value", "10" }, {"currency", "USD" } }
                    },
                    new ChargeModel()
                    {
                        ChargeType = "TAX",
                        ChargeAmount = new Dictionary<string, string>(){{ "value", "12" }, { "currency", "USD" } }
                    },
                }
            });
            Plan.PaymentDef.Add(
            new PaymentDefinition()
            {
                Name = "Regular payment definition",
                PaymentType = "TRIAL",
                Frequency = "WEEK",
                FrequencyInterval = "5",
                Amount = new Dictionary<string, string>() { { "value", "9.19" }, { "currency", "USD" } },
                Cycles = "12",
                ChargeModels = new List<ChargeModel>()
                {
                    new ChargeModel()
                    {
                        ChargeType = "SHIPPING",
                        ChargeAmount = new Dictionary<string, string>() {{ "value", "1" }, {"currency", "USD" } }
                    },
                    new ChargeModel()
                    {
                        ChargeType = "TAX",
                        ChargeAmount = new Dictionary<string, string>(){{ "value", "1" }, { "currency", "USD" } }
                    },
                }
            });

            Plan.MerchantPref.SetupFee = new Dictionary<string, string>() { { "value", "1" }, { "currency", "USD" } };
            Plan.MerchantPref.ReturnUrl = "https://payment.eb-test.info/paymentreturn/paypalreturn?res=accept&tok=" + payPalOauth.AccessToken;
            Plan.MerchantPref.CancelUrl = "https://payment.eb-test.info/paymentreturn/paypalreturn?res=cancel";
            Plan.MerchantPref.AutoBillAmount = "YES";
            Plan.MerchantPref.InitialFailAmountAction = "CONTINUE";
            Plan.MerchantPref.MaxFailAttempts = "0";


            //PREPARING REQUEST TO CREATE BILLING PLAN
            string JsonBody = JsonConvert.SerializeObject(Plan, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
            
            FlurlRequest flurlRequest = new FlurlRequest(UriString + "v1/payments/billing-plans/");
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
            string Url = UriString + "v1/payments/billing-plans/" + PaymentPlanID + "/";
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

        [Authenticate]
        public PayPalPaymentResponse Get(PayPalPaymentRequest req)
        {
            PayPalPaymentResponse resp = new PayPalPaymentResponse();

            PayPalOauthObject payPalOauth = CreateOAuthToken();

            FlurlClient flurlClient = new FlurlClient(UriString);
            flurlClient.Headers.Add("Content-Type", "application/json");
            flurlClient.Headers.Add("Authorization", "Bearer " + payPalOauth.AccessToken);


            //CREATING BILLING PLAN && //ACTIVATING THE BILLING PLAN
            BillingPlanResponse BillPlanResponse = GetBillingPlan(payPalOauth, flurlClient);


            //CREATING AND PARSING THE BILLING AGREEMENT
            //Object Initializations
            //string PlanObject = "id : \"" + "\"" + PaymentPlanID;

            FundingInstrument fundingInstrument = new FundingInstrument();
            fundingInstrument.CardDetails = new CreditCard()
            {
                CardNumber = 4868693532126484.ToString(),
                Cvv2 = 568,
                CardType = "visa",
                ExpireMonth = 10,
                ExpireYear = 2026,
            };

            BillingAgreementRequest BillAgreementRequest = new BillingAgreementRequest()
            {
                Name = "Trial Billing Request",
                Description = "Trial billing agreement",
                StartDate = "2020-01-01T09:13:49Z",
                BillingPlan = new BillingPlanResponse(BillPlanResponse.PlanID),
                Payer = new PayerDetails()
                {
                    PayMethod = PayerDetails.PaymentMethodsStrings[(int)PaymentMethod.paypal],
                    FundingInstruments = null,
                    FundingOptionId = null
                }
            };


            string AgreementUrl = UriString + "v1/payments/billing-agreements/";
            FlurlRequest AgreementRequest = new FlurlRequest(AgreementUrl);
            AgreementRequest.Client = flurlClient;

            string AgreementJson = JsonConvert.SerializeObject(BillAgreementRequest, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
            var AgreementResult = Send(HttpMethod.Post, AgreementRequest, AgreementJson).Result;
            var AgreementResConents = GetResponseContents(AgreementResult).Result;
            Console.WriteLine("\n\nBilling Agreement Creating Request :: ");
            Console.WriteLine("Response Code :: " + AgreementResult.StatusCode);
            Console.WriteLine("Response :: " + AgreementResConents);
            string AcceptUrl = AgreementResConents.Substring(AgreementResConents.IndexOf("href") + 7, 94);
            Console.WriteLine("Accept URL :: " + AcceptUrl);

            resp.Test = AcceptUrl;
            return resp;
        }
    }
}
