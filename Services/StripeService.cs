using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack.Stripe;
using ServiceStack.Stripe.Types;
using System;
using System.Collections.Generic;
using System.Data.Common;
using Stripe;
using ExpressBase.Common.Stripe;

namespace ExpressBase.ServiceStack.Services
{
    public class StripeService : EbBaseService
    {
        public StripeService(IEbConnectionFactory _dbf) : base(_dbf) { }
        public StripeGateway gateway = new StripeGateway(Environment.GetEnvironmentVariable(EnvironmentConstants.EB_STRIPE_SECRET_KEY));
        public static int i = 1;
        public const string USD = "USD";


        public CheckCustomerResponse Post(CheckCustomerRequest request)
        {
            CheckCustomerResponse resp = new CheckCustomerResponse();
            //string custid = "";
            try
            {
                using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                {
                    con.Open();
                    string str = string.Format(@"
                        SELECT COUNT(*)
                        FROM
                            eb_customer 
                        WHERE 
                            email='{0}' ", request.EmailId);

                    DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str);

                    Int64 cnt = (Int64)cmd.ExecuteScalar();

                    if (cnt == 0)
                    {
                        resp.Status = false;
                    }
                    else
                    {
                        resp.Status = true;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in Customer Check : " + e.StackTrace);
            }

            return resp;
        }


        public CheckCustomerSubscribedResponse Post(CheckCustomerSubscribedRequest request)
        {
            CheckCustomerSubscribedResponse resp = new CheckCustomerSubscribedResponse();
            //string custid = "";
            try
            {
                using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                {
                    con.Open();
                    string str2 = string.Format(@"
                            SELECT 
                                cust_id
                            FROM    
                                eb_customer
                            WHERE   
                                user_id= {0} and solution_id = '{1}' ", request.UserId, request.SolnId);
                    EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str2);
                    if (dt != null && dt.Rows.Count > 0)
                    {
                        string cust_id = dt.Rows[0][0].ToString();
                        string str = string.Format(@"
                                        SELECT plan_id, user_no 
                                        FROM eb_subscription 
                                        WHERE cust_id = '{0}'", cust_id);
                        EbDataTable dt1 = InfraConnectionFactory.DataDB.DoQuery(str);
                        if (dt1 != null && dt1.Rows.Count > 0)
                        {
                            resp.Plan = dt1.Rows[0][0].ToString();
                            resp.Users = int.Parse(dt1.Rows[0][1].ToString());
                            resp.CustId = cust_id;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in Customer Check : " + e.StackTrace);
            }

            return resp;
        }

        public CreateCustomerResponse Post(CreateCustomerRequest request)
        {
            CreateCustomerResponse resp = new CreateCustomerResponse();
            //string custid = "";
            try
            {
                using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                {
                    con.Open();
                    string str = string.Format(@"
                        SELECT COUNT(*)
                        FROM
                            eb_customer 
                        WHERE 
                            email='{0}' ", request.EmailId);

                    DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str);

                    Int64 cnt = (Int64)cmd.ExecuteScalar();

                    if (cnt == 0)
                    {
                        StripeCustomer customer = gateway.Post(new CreateStripeCustomerWithToken
                        {
                            AccountBalance = 0000,
                            Card = request.TokenId,
                            Description = "Description",
                            Email = request.EmailId,
                        });
                        resp.CustomerId = customer.Id;
                        string str1 = @"
                            INSERT INTO
                                eb_customer (cust_id,email,user_id,solution_id,created_at)
                            VALUES (@custid,@email,@userid,@solutionid,@createdat)";

                        DbCommand cmd1 = InfraConnectionFactory.DataDB.GetNewCommand(con, str1);

                        cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@custid", Common.Structures.EbDbTypes.String, customer.Id));
                        cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@email", Common.Structures.EbDbTypes.String, request.EmailId));
                        cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@userid", Common.Structures.EbDbTypes.Int16, request.UserId));
                        cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@solutionid", Common.Structures.EbDbTypes.String, request.SolnId));
                        cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@createdat", Common.Structures.EbDbTypes.DateTime, DateTime.Now));
                        cmd1.ExecuteNonQuery();


                    }
                    else
                    {
                        string str2 = string.Format(@"
                            SELECT 
                                cust_id
                            FROM    
                                eb_customer
                            WHERE   
                                email='{0}' ", request.EmailId);

                        EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str2);
                        //DbCommand cmd2 = InfraConnectionFactory.DataDB.GetNewCommand(con, str2);
                        //DbDataReader dr = cmd2.ExecuteReader();
                        //while (dr.Read())
                        //{
                        //    custid = dr[0].ToString();
                        //}
                        //resp.CustomerId = custid;
                        resp.CustomerId = dt.Rows[0][0].ToString();
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in Stripe Customer Creation : " + e.Message + e.StackTrace);
            }

            return resp;
        }

        public GetCustomerResponse Post(GetCustomerRequest request)
        {
            GetCustomerResponse resp = new GetCustomerResponse();
            string str = string.Format(@"
                        SELECT name,address1,address2,city,state,country,email
                        FROM eb_customer 
                        WHERE cust_id = '{0}'", request.CustId);
            EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
            resp.Name = dt.Rows[0][0].ToString();
            resp.Address = dt.Rows[0][1].ToString();
            resp.Zip = dt.Rows[0][2].ToString();
            resp.City = dt.Rows[0][3].ToString();
            resp.State = dt.Rows[0][4].ToString();
            resp.Country = dt.Rows[0][5].ToString();
            resp.Email = dt.Rows[0][6].ToString(); ;
            return resp;
        }

        public GetCardResponse Post(GetCardRequest request)
        {
            GetCardResponse resp = new GetCardResponse();
            string str = string.Format(@"
                        SELECT card_id 
                        FROM eb_card 
                        WHERE cust_id = '{0}'", request.CustId);
            EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
            string card_id = dt.Rows[0][0].ToString();

            StripeConfiguration.SetApiKey(Environment.GetEnvironmentVariable(EnvironmentConstants.EB_STRIPE_SECRET_KEY));

            var service = new CardService();
            Card response = service.Get(request.CustId, card_id);
            resp.Last4 = response.Last4;
            resp.ExpMonth = response.ExpMonth;
            resp.ExpYear = response.ExpYear;
            return resp;
        }

        public void Post(UpdateCardRequest request)
        {
            //UpdateCardResponse resp = new UpdateCardResponse();

            StripeCard card = gateway.Post(new UpdateStripeCard
            {
                CustomerId = request.CustId,
                CardId = request.CardId,
                Name = request.Name,
                AddressLine1 = request.Address,
                AddressZip = request.Zip,
                AddressCity = request.City,
                AddressState = request.State,
                AddressCountry = request.Country,

            });

            using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                string str = @"
                    UPDATE 
                        eb_customer
                    SET 
                        name=@name, address1=@add1,address2=@add2, city=@city, state=@state, country=@country 
                    WHERE 
                        cust_id=@custid";

                DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str);

                cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@custid", Common.Structures.EbDbTypes.String, request.CustId));
                cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@name", Common.Structures.EbDbTypes.String, request.Name));
                cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@add1", Common.Structures.EbDbTypes.String, request.Address));
                cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@add2", Common.Structures.EbDbTypes.String, request.Zip));
                cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@city", Common.Structures.EbDbTypes.String, request.City));
                cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@state", Common.Structures.EbDbTypes.String, request.State));
                cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@country", Common.Structures.EbDbTypes.String, request.Country));
                cmd.ExecuteNonQuery();

                string str1 = string.Format(@"
                    SELECT COUNT(*) 
                    FROM
                        eb_card 
                    WHERE 
                        cust_id='{0}' 
                    AND 
                        card_id='{1}' ", request.CustId, request.CardId);

                DbCommand cmd1 = InfraConnectionFactory.DataDB.GetNewCommand(con, str1);

                Int64 cnt = (Int64)cmd1.ExecuteScalar();

                if (cnt == 0)
                {
                    string str2 = @"
                        INSERT INTO 
                            eb_card (cust_id,card_id,created_at)
                        VALUES (@custid,@cardid,@createdat)";

                    DbCommand cmd2 = InfraConnectionFactory.DataDB.GetNewCommand(con, str2);

                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@custid", Common.Structures.EbDbTypes.String, request.CustId));
                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@cardid", Common.Structures.EbDbTypes.String, request.CardId));
                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@createdat", Common.Structures.EbDbTypes.DateTime, DateTime.Now));
                    cmd2.ExecuteNonQuery();
                }
            }
        }

        public void Post(CreateChargeRequest request)
        {
            //CreateChargeResponse resp = new CreateChargeResponse();

            StripeCharge charge = gateway.Post(new ChargeStripeCustomer
            {
                Amount = 100,
                Customer = request.CustId,
                Currency = "usd",
                Description = "Test Charge Customer",
            });

        }

        public void Post(CreateCharge2Request request)
        {
            CreateChargeResponse resp = new CreateChargeResponse();

            StripeCharge charge = gateway.Post(new ChargeStripeCustomer
            {
                Amount = int.Parse(request.Total) * 100,
                Customer = request.CustId,
                Currency = "usd",
                Description = "Test Charge Customer",
            });


            //        var options = new SessionCreateOptions
            //        {
            //            PaymentMethodTypes = new List<string> {
            //    "card",
            //},
            //            LineItems = new List<SessionLineItemOptions> {
            //    new SessionLineItemOptions {
            //        Name = "T-shirt",
            //        Description = "Comfortable cotton t-shirt",
            //        Amount = 500,
            //        Currency = "usd",
            //        Quantity = 1,
            //    },
            //},
            //            SuccessUrl = "https://example.com/success",
            //            CancelUrl = "https://example.com/cancel",
            //        };

            //        var service = new SessionService();
            //        Session session = service.Create(options);

        }

        public CreatePlanResponse Post(CreatePlanRequest request)
        {
            CreatePlanResponse resp = new CreatePlanResponse();
            int amt = int.Parse(request.Total);
            string planid = "PLAN-01-" + amt + "-" + request.Interval + "-" + request.Interval_count;

            using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                string str = string.Format(@"
                    SELECT COUNT(*)
                    FROM 
                        eb_plan
                    WHERE 
                        plan_id = '{0}'", planid);

                DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str);

                Int64 cnt = (Int64)cmd.ExecuteScalar();

                if (cnt > 0)
                {
                    resp.PlanId = planid;
                }
                else
                {
                    StripePlan plan = gateway.Post(new CreateStripePlan
                    {
                        Id = planid,
                        Amount = (amt * 100),
                        Currency = "usd",
                        //Name = "Test Plan",
                        Interval = (StripePlanInterval)request.Interval,
                        IntervalCount = request.Interval_count,
                        Product = new StripePlanProduct { Name = "Test Plan" }
                    });

                    string str2 = @"
                                    INSERT INTO 
                                            eb_plan (plan_id,amount,currency,interval,interval_count,created_at)
                                    VALUES 
                                            (@planid, @amt,@curr,@interval,@interval_cnt,@createdat)";

                    DbCommand cmd2 = InfraConnectionFactory.DataDB.GetNewCommand(con, str2);

                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@planid", Common.Structures.EbDbTypes.String, plan.Id));
                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@amt", Common.Structures.EbDbTypes.Decimal, amt));
                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@curr", Common.Structures.EbDbTypes.String, plan.Currency));
                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@interval", Common.Structures.EbDbTypes.String, Enum.GetName(typeof(StripePlanInterval), plan.Interval)));
                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@interval_cnt", Common.Structures.EbDbTypes.Int16, plan.IntervalCount));
                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@createdat", Common.Structures.EbDbTypes.DateTime, DateTime.Now));
                    cmd2.ExecuteNonQuery();

                    i++;
                    resp.PlanId = plan.Id;
                }
            }
            return resp;
        }

        public GetPlansResponse Post(GetPlansRequest request)
        {
            GetPlansResponse resp = new GetPlansResponse();

            StripeCollection<StripePlan> plans = gateway.Get(new GetStripePlans
            {
                Limit = 40
            });

            List<Eb_StripePlans> Plans = new List<Eb_StripePlans>();
            int count = plans.Data.Count;
            for (int i = 0; i < count; i++)
            {
                Plans.Add(new Eb_StripePlans
                {
                    Amount = plans.Data[i].Amount,
                    Currency = plans.Data[i].Currency,
                    Id = plans.Data[i].Id,
                    Interval = plans.Data[i].Interval,
                    Interval_count = plans.Data[i].IntervalCount
                });
            }
            resp.Plans = new Eb_StripePlansList
            {
                Plans = Plans
            };

            return resp;
        }

        public CreateCouponResponse Post(CreateCouponRequest request)
        {
            CreateCouponResponse resp = new CreateCouponResponse();
            string couponid = "COUPON-" + request.Duration + "-" + request.PercentageOff + "-" + request.DurationInMonth + "-" + request.RedeemBy + "-" + request.MaxRedeem;

            using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                string str = String.Format(@"
                    SELECT COUNT(*)
                    FROM
                        eb_coupon
                    WHERE 
                        coupon_id = '{0}'", couponid);

                DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str);
                Int64 cnt = (Int64)cmd.ExecuteScalar();
                if (cnt > 0)
                {
                    resp.CouponId = couponid;
                }
                else
                {
                    StripeCoupon coupon = gateway.Post(new CreateStripeCoupon
                    {
                        Id = couponid,
                        Duration = (StripeCouponDuration)request.Duration,
                        PercentOff = request.PercentageOff,
                        Currency = USD,
                        DurationInMonths = request.DurationInMonth,
                        RedeemBy = DateTime.UtcNow.AddYears(request.RedeemBy),
                        MaxRedemptions = request.MaxRedeem,
                    });

                    string str1 = @"
                        INSERT INTO
                            eb_coupon (coupon_id,duration,percentage_off,currency,dur_in_months,max_redeem,created_at)
                        VALUES 
                            (@coupid,@dur,@peroff,@curr,@durmon,@maxred,@createdat)";

                    DbCommand cmd1 = InfraConnectionFactory.DataDB.GetNewCommand(con, str1);
                    cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@coupid", Common.Structures.EbDbTypes.String, couponid));
                    cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@dur", Common.Structures.EbDbTypes.Decimal, coupon.Duration));
                    cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@peroff", Common.Structures.EbDbTypes.Int16, coupon.PercentOff));
                    cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@curr", Common.Structures.EbDbTypes.String, USD));
                    cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@durmon", Common.Structures.EbDbTypes.Int16, coupon.DurationInMonths));
                    cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@maxred", Common.Structures.EbDbTypes.Int16, coupon.MaxRedemptions));
                    cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@createdat", Common.Structures.EbDbTypes.DateTime, DateTime.Now));
                    cmd1.ExecuteNonQuery();
                }
            }

            resp.CouponId = couponid;
            return resp;
        }

        public CreateSubscriptionResponse Post(CreateSubscriptionRequest request)
        {
            CreateSubscriptionResponse resp = new CreateSubscriptionResponse();
            using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                //string str = string.Format("select count(*) from eb_subscription where cust_id = '{0}'and plan_id = '{1}' and coupon_id = '{2}'", request.CustId,request.PlanId,request.CoupId);
                //DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str);
                //Int64 cnt = (Int64)cmd.ExecuteScalar();
                //if (cnt > 0)
                //{
                //    // resp.CouponId = couponid;
                //}
                //else
                //{
                //StripeSubscription subscription = gateway.Post(new SubscribeStripeCustomer
                //{
                //    CustomerId = request.CustId,
                //    Plan = request.PlanId,
                //    Coupon = request.CoupId,
                //    Quantity = 1
                //});

                StripeConfiguration.SetApiKey(Environment.GetEnvironmentVariable(EnvironmentConstants.EB_STRIPE_SECRET_KEY));
                var items = new List<SubscriptionItemOption>
                {
                    new SubscriptionItemOption
                    {
                        PlanId = request.PlanId
                    }
                };
                var options = new SubscriptionCreateOptions
                {
                    CustomerId = request.CustId,
                    Items = items
                };

                var service = new SubscriptionService();
                Subscription subscription = service.Create(options);

                string inv_id = subscription.LatestInvoiceId;
                string sub_item_id = subscription.Items.Data[0].Id;

                //var service1 = new InvoiceService();
                //var invoice = service1.Get(inv_id);
                //string url = invoice.HostedInvoiceUrl;
                //return url;


                var usageRecordOptions = new UsageRecordCreateOptions()
                {
                    Quantity = request.Total,
                    Timestamp = DateTime.Now.AddMinutes(3),
                    Action = "increment"
                };
                var usageRecordService = new UsageRecordService();
                UsageRecord usageRecord = usageRecordService.Create(sub_item_id, usageRecordOptions);
                string str = @"
                        UPDATE 
                            eb_solutions
                        SET 
                            pricing_tier = @pricingtier
                        WHERE
                            esolution_id = @solid";
                DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str);
                cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@pricingtier", Common.Structures.EbDbTypes.Int16, (int)PricingTiers.STANDARD));
                cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@solid", Common.Structures.EbDbTypes.String, request.SolnId));
                cmd.ExecuteNonQuery();
                TenantUserServices _tenantUserService = base.ResolveService<TenantUserServices>();
                _tenantUserService.Post(new UpdateSolutionRequest() { SolnId = request.SolnId, UserId = request.UserId });
                string str1 = @"
                    INSERT INTO
                        eb_subscription (cust_id,plan_id,coupon_id,sub_id,sub_item_id,latest_invoice_id,user_no,created_at)
                    VALUES (@custid,@planid,@coupid,@subid,@subitemid,@invid,@userno,@createdat)";

                DbCommand cmd1 = InfraConnectionFactory.DataDB.GetNewCommand(con, str1);
                cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@custid", Common.Structures.EbDbTypes.String, request.CustId));
                cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@planid", Common.Structures.EbDbTypes.String, request.PlanId));
                cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@coupid", Common.Structures.EbDbTypes.String, request.CoupId));
                cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@subid", Common.Structures.EbDbTypes.String, subscription.Id));
                cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@subitemid", Common.Structures.EbDbTypes.String, sub_item_id));
                cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@invid", Common.Structures.EbDbTypes.String, inv_id));
                cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@userno", Common.Structures.EbDbTypes.Int16, request.Total));
                cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@createdat", Common.Structures.EbDbTypes.DateTime, DateTime.Now));
                cmd1.ExecuteNonQuery();
                //}

                resp.PeriodStart = ((DateTime)subscription.CurrentPeriodStart).ToString("dd MMM,yyyy");
                resp.PeriodEnd = ((DateTime)subscription.CurrentPeriodEnd).ToString("dd MMM,yyyy");
                resp.Created = ((DateTime)subscription.Created).ToString("dd MMM,yyyy");
                resp.Amount = (subscription.Plan.Amount/100);
                resp.UseageType = subscription.Plan.UsageType;
                resp.BillingScheme = subscription.Plan.BillingScheme;
                resp.Quantity = usageRecord.Quantity;
                resp.Plan = subscription.Plan.Nickname;
            }

            return resp;
        }

        public UpgradeSubscriptionResponse Post(UpgradeSubscriptionRequest request)
        {
            UpgradeSubscriptionResponse resp = new UpgradeSubscriptionResponse();
            using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                // Retriving cust_id from eb_customer table
                string str2 = string.Format(@"
                        SELECT cust_id 
                        FROM eb_customer 
                        WHERE solution_id = '{0}' and user_id = {1}", request.SolnId, request.UserId);
                EbDataTable dt1 = InfraConnectionFactory.DataDB.DoQuery(str2);
                string cust_id = dt1.Rows[0][0].ToString();
                // Retriving subscription id and item id from eb_subscription table
                string str = string.Format(@"
                        SELECT sub_id, sub_item_id 
                        FROM eb_subscription 
                        WHERE cust_id = '{0}'", cust_id);
                EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
                string sub_id = dt.Rows[0][0].ToString();
                string sub_item_id = dt.Rows[0][1].ToString();
                // Updating Usage Record
                var usageRecordOptions = new UsageRecordCreateOptions()
                {
                    Quantity = request.Total,
                    Timestamp = DateTime.Now.AddMinutes(3),
                    Action = "increment"
                };
                var usageRecordService = new UsageRecordService();
                UsageRecord usageRecord = usageRecordService.Create(sub_item_id, usageRecordOptions);
                //Retriving subscription details
                var service = new SubscriptionService();
                var subscription = service.Get(sub_id);
                // Updating Subscription Table
                string str1 = @"
                    UPDATE eb_subscription 
                    SET user_no = @users, updated_at = @updatedat
                    WHERE sub_id = @subid";
                DbCommand cmd1 = InfraConnectionFactory.DataDB.GetNewCommand(con, str1);
                cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@subid", Common.Structures.EbDbTypes.String, sub_id));
                cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@users", Common.Structures.EbDbTypes.Int16, request.Total));
                cmd1.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@updatedat", Common.Structures.EbDbTypes.DateTime, DateTime.Now));
                cmd1.ExecuteNonQuery();
                //passing values to object
                resp.PeriodStart = ((DateTime)subscription.CurrentPeriodStart).ToString("dd MMM,yyyy");
                resp.PeriodEnd = ((DateTime)subscription.CurrentPeriodEnd).ToString("dd MMM,yyyy");
                resp.Created = ((DateTime)subscription.Created).ToString("dd MMM,yyyy");
                resp.Amount = (subscription.Plan.Amount / 100);
                resp.UseageType = subscription.Plan.UsageType;
                resp.BillingScheme = subscription.Plan.BillingScheme;
                resp.Quantity = usageRecord.Quantity;
                resp.Plan = subscription.Plan.Nickname;
            }

            return resp;
        }

        public void Post(CreateInvoiceRequest request)
        {
            CreateInvoiceResponse resp = new CreateInvoiceResponse();
            int amt = int.Parse(request.Total);
            StripeInvoice invoice = gateway.Post(new CreateStripeInvoice
            {
                Customer = request.CustId,
                ApplicationFee = (amt * 100),
            });
        }

        public void Post(StripewebhookRequest request)
        {
            const string secret = "whsec_GqJuzEFUWI3I3ylB0aPTDax5mIWn2jR9";
            //var stripeEvent = EventUtility.ParseEvent(json);
            var json = request.Json;

            try
            {
                var stripeEvent = EventUtility.ConstructEvent(json,
                    Request.Headers["Stripe-Signature"], secret);

                string stripeevent = stripeEvent.Type;
                string type = stripeEvent.Data.Object.Object;
                string type_id = "";

                if (stripeEvent.Type == Events.CustomerCreated)
                {
                    Customer cc = stripeEvent.Data.Object as Customer;
                    type_id = cc.Id;
                }
                else if (stripeEvent.Type == Events.CustomerSubscriptionCreated)
                {
                    Customer cc = stripeEvent.Data.Object as Customer;
                    type_id = cc.Id;
                }
                else if (stripeEvent.Type == Events.CustomerDiscountCreated)
                {
                    Customer cc = stripeEvent.Data.Object as Customer;
                    type_id = cc.Id;
                }
                else if (stripeEvent.Type == Events.CustomerUpdated)
                {
                    Customer cc = stripeEvent.Data.Object as Customer;
                    type_id = cc.Id;
                }
                else if (stripeEvent.Type == Events.ChargeSucceeded)
                {
                    Charge cc = stripeEvent.Data.Object as Charge;
                    type_id = cc.Id;
                }
                else if (stripeEvent.Type == Events.PlanCreated)
                {
                    Plan cc = stripeEvent.Data.Object as Plan;
                    type_id = cc.Id;
                }
                else if (stripeEvent.Type == Events.CustomerSourceUpdated)
                {
                    Customer cc = stripeEvent.Data.Object as Customer;
                    type_id = cc.Id;
                }
                else if (stripeEvent.Type == Events.CustomerSourceCreated)
                {
                    Customer cc = stripeEvent.Data.Object as Customer;
                    type_id = cc.Id;
                }
                else if (stripeEvent.Type == Events.CustomerDiscountCreated)
                {
                    Customer cc = stripeEvent.Data.Object as Customer;
                    type_id = cc.Id;
                }

                using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                {
                    con.Open();

                    string str = string.Format(@"
                        INSERT INTO 
                            eb_stripeevents (event,type,type_id,created_at)
                        VALUES('{0}','{1}','{2}','{3}')", stripeevent, type, type_id, DateTime.Now);

                    DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str);

                    cmd.ExecuteNonQuery();
                }

            }
            catch (Exception)
            {

                //return BadRequest();
            }
        }

        public GetCustomerInvoiceResponse Post(GetCustomerInvoiceRequest request)
        {
            GetCustomerInvoiceResponse resp = new GetCustomerInvoiceResponse();
            StripeConfiguration.SetApiKey(Environment.GetEnvironmentVariable(EnvironmentConstants.EB_STRIPE_SECRET_KEY));

            StripeCollection<StripeInvoice> invoices = gateway.Get(new GetStripeInvoices
            {
                Customer = request.CustId
            });

            StripeCustomer customer = gateway.Get(new GetStripeCustomer
            {
                Id = request.CustId
            });

            var service1 = new InvoiceService();


            int count = invoices.Data.Count;
            List<Eb_StripeInvoice> List = new List<Eb_StripeInvoice>();
            for (int i = 0; i < count; i++)
            {
                var invoice = service1.Get(invoices.Data[i].Id);
                List.Add(new Eb_StripeInvoice
                {
                    Id = invoices.Data[i].Id,
                    PlanId = invoices.Data[i].Lines.Data[0].Plan.Id,
                    Amount = invoices.Data[i].Lines.Data[0].Plan.Amount / 100,
                    Date = invoices.Data[i].Date,
                    SubTotal = invoices.Data[i].Subtotal / 100,
                    Total = invoices.Data[i].Total / 100,
                    Type = invoices.Data[i].Lines.Data[0].Type,
                    Description = invoices.Data[i].Lines.Data[0].Description,
                    Url = invoice.HostedInvoiceUrl,
                    InvNumber = invoice.Number,
                    Status = invoice.Paid,
                    Currency = invoices.Data[i].Lines.Data[0].Currency,
                    Quantity = invoices.Data[i].Lines.Data[0].Quantity,
                    PeriodStart = invoices.Data[i].PeriodStart,
                    PeriodEnd = invoices.Data[i].PeriodEnd
                });

            }
            resp.Invoices = new Eb_StripeInvoiceList
            {
                List = List
            };

            return resp;
        }

        public GetCustomerUpcomingInvoiceResponse Post(GetCustomerUpcomingInvoiceRequest request)
        {
            GetCustomerUpcomingInvoiceResponse resp = new GetCustomerUpcomingInvoiceResponse();

            StripeInvoice Inv = gateway.Get(new GetUpcomingStripeInvoice
            {
                Customer = request.CustId,
            });



            int count = Inv.Lines.Data.Count;
            List<Eb_StripeUpcomingInvoice> Data = new List<Eb_StripeUpcomingInvoice>();
            for (int i = 0; i < count; i++)
            {
                Data.Add(new Eb_StripeUpcomingInvoice
                {
                    Amount = Inv.Lines.Data[i].Amount / 100,
                    Type = Inv.Lines.Data[i].Type,
                    Description = Inv.Lines.Data[i].Description,
                    PeriodEnd = Inv.Lines.Data[i].Period.End,
                    PeriodStart = Inv.Lines.Data[i].Period.Start,
                    Quantity = Inv.Lines.Data[i].Quantity,
                    PlanId = Inv.Lines.Data[i].Plan.Id,
                });
            }

            resp.Invoice = new Eb_StripeUpcomingInvoiceList
            {
                Total = Inv.AmountDue / 100,
                Date = Inv.Date,
                Currency = Inv.Currency,
                PercentOff = Inv.Discount == null ? 0 : Inv.Discount.Coupon.PercentOff,
                CouponId = Inv.Discount == null ? "" : Inv.Discount.Coupon.Id,
                Data = Data
            };

            return resp;
        }
    }


}
