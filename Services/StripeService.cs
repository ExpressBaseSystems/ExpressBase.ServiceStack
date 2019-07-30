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
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

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
                        if (dt != null && dt.Rows.Count > 0)
                        {
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
            StripeConfiguration.ApiKey = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_STRIPE_SECRET_KEY);
            string str = string.Format(@"
                        SELECT name,address1,zip,city,state,country,email
                        FROM eb_customer 
                        WHERE cust_id = '{0}'", request.CustId);
            EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
            if (dt != null && dt.Rows.Count > 0)
            {
                resp.Name = dt.Rows[0][0].ToString();
                resp.Address = dt.Rows[0][1].ToString();
                resp.Zip = dt.Rows[0][2].ToString();
                resp.City = dt.Rows[0][3].ToString();
                resp.State = dt.Rows[0][4].ToString();
                resp.Country = dt.Rows[0][5].ToString();
                resp.Email = dt.Rows[0][6].ToString();
            }
            var service = new CustomerService();
            var customer = service.Get(request.CustId);
            resp.DefaultSourceId = customer.DefaultSourceId;
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
            string card_id = "";
            List<Eb_StripeCards> Card = new List<Eb_StripeCards>();
            if (dt != null && dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    card_id = dt.Rows[i][0].ToString();
                    StripeConfiguration.ApiKey = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_STRIPE_SECRET_KEY);
                    var service = new CardService();
                    Card response = service.Get(request.CustId, card_id);
                    Card.Add(new Eb_StripeCards
                    {
                        CardId = card_id,
                        Last4 = response.Last4,
                        ExpMonth = response.ExpMonth,
                        ExpYear = response.ExpYear,
                    });
                }
            }
            resp.Cards = new Eb_StripeCardsList
            {
                Card = Card
            };
            resp.Count = dt.Rows.Count;
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
                        name=@name, address1=@add1,zip=@add2, city=@city, state=@state, country=@country 
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
                            eb_card (cust_id,token_id,card_id,created_at)
                        VALUES (@custid,@tokenid,@cardid,@createdat)";

                    DbCommand cmd2 = InfraConnectionFactory.DataDB.GetNewCommand(con, str2);

                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@custid", Common.Structures.EbDbTypes.String, request.CustId));
                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@tokenid", Common.Structures.EbDbTypes.String, request.TokenId));
                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@cardid", Common.Structures.EbDbTypes.String, request.CardId));
                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@createdat", Common.Structures.EbDbTypes.DateTime, DateTime.Now));
                    cmd2.ExecuteNonQuery();
                }
            }
        }

        public UpdateCustomerCardResponse Post(UpdateCustomerCardRequest request)
        {
            UpdateCustomerCardResponse resp = new UpdateCustomerCardResponse();
            using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                string str = string.Format(@"
                    SELECT card_id
                    FROM eb_card
                    WHERE cust_id = '{0}'", request.CustId);

                EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
                if (dt != null && dt.Rows.Count > 0)
                {
                    for (int i = 0; i < dt.Rows.Count; i++)
                    {
                        string card_id = dt.Rows[i][0].ToString();
                        StripeCard card = gateway.Post(new UpdateStripeCard
                        {
                            CustomerId = request.CustId,
                            CardId = card_id,
                            Name = request.Name,
                            AddressLine1 = request.Address,
                            AddressZip = request.Zip,
                            AddressCity = request.City,
                            AddressState = request.State,
                            AddressCountry = request.Country
                        });
                        string str1 = @"
                                UPDATE 
                                    eb_customer
                                SET 
                                    name=@name, address1=@add1,zip=@add2, city=@city, state=@state, country=@country 
                                WHERE 
                                    cust_id=@custid";

                        DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str1);

                        cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@custid", Common.Structures.EbDbTypes.String, request.CustId));
                        cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@name", Common.Structures.EbDbTypes.String, request.Name));
                        cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@add1", Common.Structures.EbDbTypes.String, request.Address));
                        cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@add2", Common.Structures.EbDbTypes.String, request.Zip));
                        cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@city", Common.Structures.EbDbTypes.String, request.City));
                        cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@state", Common.Structures.EbDbTypes.String, request.State));
                        cmd.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@country", Common.Structures.EbDbTypes.String, request.Country));
                        cmd.ExecuteNonQuery();
                    }
                    resp.Name = request.Name;
                    resp.Address = request.Address;
                    resp.City = request.City;
                    resp.State = request.State;
                    resp.Country = request.Country;
                    resp.Zip = request.Zip;
                }
            }
            return resp;
        }

        public AddCustomerCardResponse Post(AddCustomerCardRequest request)
        {
            AddCustomerCardResponse resp = new AddCustomerCardResponse();
            StripeConfiguration.ApiKey = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_STRIPE_SECRET_KEY);
            using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                string str = string.Format(@"
                    SELECT name,address1,zip,city,state,country
                    FROM eb_customer
                    WHERE cust_id = '{0}'", request.CustId);

                EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
                if (dt != null && dt.Rows.Count > 0)
                {
                    var options = new CardCreateOptions
                    {
                        Source = request.TokenId
                    };
                    var service = new CardService();
                    var card = service.Create(request.CustId, options);

                    string str2 = @"
                        INSERT INTO 
                            eb_card (cust_id,token_id,card_id,created_at)
                        VALUES (@custid,@tokenid,@cardid,@createdat)";

                    DbCommand cmd2 = InfraConnectionFactory.DataDB.GetNewCommand(con, str2);

                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@custid", Common.Structures.EbDbTypes.String, request.CustId));
                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@tokenid", Common.Structures.EbDbTypes.String, request.TokenId));
                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@cardid", Common.Structures.EbDbTypes.String, request.CardId));
                    cmd2.Parameters.Add(InfraConnectionFactory.DataDB.GetNewParameter("@createdat", Common.Structures.EbDbTypes.DateTime, DateTime.Now));
                    cmd2.ExecuteNonQuery();
                }
                string str1 = string.Format(@"
                        SELECT card_id 
                        FROM eb_card 
                        WHERE cust_id = '{0}'", request.CustId);
                EbDataTable dt1 = InfraConnectionFactory.DataDB.DoQuery(str1);
                string card_id = "";
                List<Eb_StripeCards> Card = new List<Eb_StripeCards>();
                if (dt1 != null && dt1.Rows.Count > 0)
                {
                    for (int i = 0; i < dt1.Rows.Count; i++)
                    {
                        card_id = dt1.Rows[i][0].ToString();
                        var service = new CardService();
                        Card response = service.Get(request.CustId, card_id);
                        Card.Add(new Eb_StripeCards
                        {
                            CardId = card_id,
                            Last4 = response.Last4,
                            ExpMonth = response.ExpMonth,
                            ExpYear = response.ExpYear,
                        });
                    }
                }
                resp.Cards = new Eb_StripeCardsList
                {
                    Card = Card
                };
                resp.Count = dt1.Rows.Count;
                var service1 = new CustomerService();
                var customer = service1.Get(request.CustId);
                resp.DefaultSourceId = customer.DefaultSourceId;
            }
            return resp;
        }

        public RemoveCustomerCardResponse Post(RemoveCustomerCardRequest request)
        {
            RemoveCustomerCardResponse resp = new RemoveCustomerCardResponse();
            StripeConfiguration.ApiKey = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_STRIPE_SECRET_KEY);
            using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                var service1 = new CustomerService();
                var customer = service1.Get(request.CustId);
                if (customer.DefaultSourceId == request.CardId)
                {
                    resp.Status = false;
                }
                else
                {
                    resp.Status = true;
                    var deletedRef = gateway.Delete(new DeleteStripeCustomerCard
                    {
                        CustomerId = request.CustId,
                        CardId = request.CardId,
                    });
                    string str = string.Format(@"
                    DELETE FROM eb_card
                    WHERE card_id = '{0}'", request.CardId);
                    DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str);
                    cmd.ExecuteNonQuery();

                    string str1 = string.Format(@"
                        SELECT card_id 
                        FROM eb_card 
                        WHERE cust_id = '{0}'", request.CustId);
                    EbDataTable dt1 = InfraConnectionFactory.DataDB.DoQuery(str1);
                    string card_id = "";
                    List<Eb_StripeCards> Card = new List<Eb_StripeCards>();
                    if (dt1 != null && dt1.Rows.Count > 0)
                    {
                        for (int i = 0; i < dt1.Rows.Count; i++)
                        {
                            card_id = dt1.Rows[i][0].ToString();
                            var service = new CardService();
                            Card response = service.Get(request.CustId, card_id);
                            Card.Add(new Eb_StripeCards
                            {
                                CardId = card_id,
                                Last4 = response.Last4,
                                ExpMonth = response.ExpMonth,
                                ExpYear = response.ExpYear,
                            });
                        }
                    }
                    resp.Cards = new Eb_StripeCardsList
                    {
                        Card = Card
                    };
                    resp.Count = dt1.Rows.Count;
                    resp.DefaultSourceId = customer.DefaultSourceId;
                }
            }
            return resp;
        }

        public EditCardExpResponse Post(EditCardExpRequest request)
        {
            EditCardExpResponse resp = new EditCardExpResponse();
            StripeConfiguration.ApiKey = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_STRIPE_SECRET_KEY);
            using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                var service1 = new CustomerService();
                var customer = service1.Get(request.CustId);

                var options = new CardUpdateOptions
                {
                    ExpMonth = request.ExpMonth,
                    ExpYear = request.ExpYear,
                };
                var service = new CardService();
                Card card = service.Update(request.CustId, request.CardId, options);


                string str1 = string.Format(@"
                        SELECT card_id 
                        FROM eb_card 
                        WHERE cust_id = '{0}'", request.CustId);
                EbDataTable dt1 = InfraConnectionFactory.DataDB.DoQuery(str1);
                string card_id = "";
                List<Eb_StripeCards> Card = new List<Eb_StripeCards>();
                if (dt1 != null && dt1.Rows.Count > 0)
                {
                    for (int i = 0; i < dt1.Rows.Count; i++)
                    {
                        card_id = dt1.Rows[i][0].ToString();
                        var service2 = new CardService();
                        Card response = service2.Get(request.CustId, card_id);
                        Card.Add(new Eb_StripeCards
                        {
                            CardId = card_id,
                            Last4 = response.Last4,
                            ExpMonth = response.ExpMonth,
                            ExpYear = response.ExpYear,
                        });
                    }
                }
                resp.Cards = new Eb_StripeCardsList
                {
                    Card = Card
                };
                resp.Count = dt1.Rows.Count;
                resp.DefaultSourceId = customer.DefaultSourceId;

            }
            return resp;
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

                StripeConfiguration.ApiKey = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_STRIPE_SECRET_KEY);
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

                TenantUserServices _tenantUserService = base.ResolveService<TenantUserServices>();
                _tenantUserService.Post(new UpdateSolutionRequest() { SolnId = request.SolnId, UserId = request.UserId });

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

        public UpgradeSubscriptionResponse Post(UpgradeSubscriptionRequest request)
        {
            StripeConfiguration.ApiKey = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_STRIPE_SECRET_KEY);
            UpgradeSubscriptionResponse resp = new UpgradeSubscriptionResponse();
            using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                // Retriving subscription id and item id from eb_subscription table
                string str = string.Format(@"
                        SELECT sub_id, sub_item_id 
                        FROM eb_subscription 
                        WHERE cust_id = '{0}'", request.CustId);
                EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(str);
                string sub_id = dt.Rows[0][0].ToString();
                string sub_item_id = dt.Rows[0][1].ToString();
                // Updating Usage Record
                var usageRecordOptions = new UsageRecordCreateOptions()
                {
                    Quantity = request.Total,
                    Timestamp = DateTime.Now,
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
            Console.WriteLine("JSON : " + request.Json);
            try
            {
                Event StripeEvent = EventUtility.ConstructEvent(request.Json,
                   request.Header, secret);
                string stripeevent = StripeEvent.Type;
                string type = StripeEvent.Data.Object.Object;
                string type_id = JsonConvert.SerializeObject(StripeEvent.Data.Object);
                var userObj = JObject.Parse(type_id);
                string cust_id = Convert.ToString(userObj["customer"]);
                Console.WriteLine("Inserting Web Hook 1: " + stripeevent + ", " + type + ", " + type_id);

                {//------------------------------------------ Account----------------------------------------------
                 //if (stripeEvent.Type == Events.AccountApplicationAuthorized)
                 //{
                 //    Customer cc = stripeEvent.Data.Object as Customer;
                 //    type_id = cc.Id;
                 //}
                 //else if (stripeEvent.Type == Events.AccountApplicationDeauthorized)
                 //{
                 //    Customer cc = stripeEvent.Data.Object as Customer;
                 //    type_id = cc.Id;
                 //}
                 //else if (stripeEvent.Type == Events.AccountExternalAccountCreated)
                 //{
                 //    Customer cc = stripeEvent.Data.Object as Customer;
                 //    type_id = cc.Id;
                 //}
                 //else if (stripeEvent.Type == Events.AccountExternalAccountDeleted)
                 //{
                 //    Customer cc = stripeEvent.Data.Object as Customer;
                 //    type_id = cc.Id;
                 //}
                 //else if (stripeEvent.Type == Events.AccountExternalAccountUpdated)
                 //{
                 //    Customer cc = stripeEvent.Data.Object as Customer;
                 //    type_id = cc.Id;
                 //}
                 //else if (stripeEvent.Type == Events.AccountUpdated)
                 //{
                 //    Customer cc = stripeEvent.Data.Object as Customer;
                 //    type_id = cc.Id;
                 //}
                 ////---------------------------Application Fee-----------------------------------------------------
                 //else if (stripeEvent.Type == Events.ApplicationFeeCreated)
                 //{
                 //    Customer cc = stripeEvent.Data.Object as Customer;
                 //    type_id = cc.Id;
                 //}
                 //else if (stripeEvent.Type == Events.ApplicationFeeRefunded)
                 //{
                 //    Customer cc = stripeEvent.Data.Object as Customer;
                 //    type_id = cc.Id;
                 //}
                 //else if (stripeEvent.Type == Events.ApplicationFeeRefundUpdated)
                 //{
                 //    Customer cc = stripeEvent.Data.Object as Customer;
                 //    type_id = cc.Id;
                 //}
                 ////--------------------------------------- Balance---------------------------------------------------
                 //else if (stripeEvent.Type == Events.BalanceAvailable)
                 //{
                 //    Customer cc = stripeEvent.Data.Object as Customer;
                 //    type_id = cc.Id;
                 //}
                 ////----------------------------------------Bitcoin----------------------------------------------------


                    ////------------------------------------------------ Charge----------------------------------------------
                    //else if (stripeEvent.Type == Events.ChargeCaptured)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ChargeDisputeClosed)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ChargeDisputeCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ChargeDisputeFundsReinstated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ChargeDisputeFundsWithdrawn)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ChargeDisputeUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ChargeExpired)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ChargeFailed)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ChargePending)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ChargeRefunded)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ChargeRefundUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ChargeSucceeded)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ChargeUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////----------------------------------Checkout -------------------------------
                    //else if (stripeEvent.Type == Events.CheckoutSessionCompleted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////----------------------------------Coupon--------------------------------------
                    //else if (stripeEvent.Type == Events.CouponCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CouponDeleted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CouponUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////---------------------------------------Credit-------------------------------------------
                    //else if (stripeEvent.Type == Events.CreditNoteCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CreditNoteUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CreditNoteVoided)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////-------------------------------Customer--------------------------------
                    //else if (stripeEvent.Type == Events.CustomerCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CustomerDeleted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CustomerDiscountCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CustomerDiscountDeleted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CustomerDiscountUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CustomerSourceCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CustomerSourceDeleted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CustomerSourceExpiring)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CustomerSourceUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CustomerSubscriptionCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CustomerSubscriptionDeleted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CustomerSubscriptionTrialWillEnd)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CustomerSubscriptionUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.CustomerUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////---------------------------------- File---------------------------------------
                    //else if (stripeEvent.Type == Events.FileCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////---------------------------------Invoice--------------------------------------
                    //else if (stripeEvent.Type == Events.InvoiceCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.InvoiceDeleted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.InvoiceFinalized)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.InvoiceItemCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.InvoiceItemDeleted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.InvoiceItemUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.InvoiceMarkedUncollectible)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.InvoicePaymentActionRequired)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.InvoicePaymentFailed)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.InvoicePaymentSucceeded)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.InvoiceSent)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.InvoiceUpcoming)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.InvoiceUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.InvoiceVoided)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////---------------------------------------Issue-----------------------------------------
                    //else if (stripeEvent.Type == Events.IssuingAuthorizationCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.IssuingAuthorizationRequest)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.IssuingAuthorizationUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.IssuingCardCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.IssuingCardholderCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.IssuingCardholderUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.IssuingCardUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.IssuingDisputeCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.IssuingDisputeUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.IssuingTransactionCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.IssuingTransactionUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////-------------------------------------- Order---------------------------------------------
                    //else if (stripeEvent.Type == Events.OrderCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.OrderPaymentFailed)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.OrderPaymentSucceeded)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.OrderReturnCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.OrderUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////------------------------------------Payment-----------------------------------
                    //else if (stripeEvent.Type == Events.PaymentIntentAmountCapturableUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PaymentIntentCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PaymentIntentPaymentFailed)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PaymentIntentSucceeded)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PaymentMethodAttached)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PaymentMethodCardAutomaticallyUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PaymentMethodDetached)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PaymentMethodUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////------------------------------------Payout--------------------------------
                    //else if (stripeEvent.Type == Events.PayoutCanceled)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PayoutCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PayoutFailed)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PayoutPaid)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PayoutUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////------------------------------------------Person-------------------------------------------
                    //else if (stripeEvent.Type == Events.PersonCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PersonDeleted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PersonUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////------------------------------------------Ping------------------------------------------------
                    //else if (stripeEvent.Type == Events.Ping)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////-------------------------------------Plan------------------------------------------------------
                    //else if (stripeEvent.Type == Events.PlanCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PlanDeleted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.PlanUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////----------------------------------------------Product-------------------------------------------
                    //else if (stripeEvent.Type == Events.ProductCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ProductDeleted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ProductUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////--------------------------------------------Recipient-------------------------------------------
                    //else if (stripeEvent.Type == Events.RecipientCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.RecipientDeleted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.RecipientUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////------------------------------------Reporting-----------------------------------
                    //else if (stripeEvent.Type == Events.ReportingReportRunFailed)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ReportingReportRunSucceeded)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ReportingReportTypeUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////---------------------------------------------Review---------------------------------------
                    //else if (stripeEvent.Type == Events.ReviewClosed)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.ReviewOpened)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////------------------------------------------Sigma---------------------------------------------
                    //else if (stripeEvent.Type == Events.SigmaScheduleQueryRunCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////------------------------------------------Sku------------------------------------------------
                    //else if (stripeEvent.Type == Events.SkuCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.SkuDeleted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.SkuUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////-----------------------------------------Source-------------------------------------------------
                    //else if (stripeEvent.Type == Events.SourceCanceled)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.SourceChargeable)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.SourceFailed)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.SourceMandateNotification)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.SourceRefundAttributesRequired)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.SourceTransactionCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.SourceTransactionUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////-------------------------------------Subscription-----------------------------------------
                    //else if (stripeEvent.Type == Events.SubscriptionScheduleAborted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.SubscriptionScheduleCanceled)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.SubscriptionScheduleCompleted)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.SubscriptionScheduleCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.SubscriptionScheduleExpiring)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.SubscriptionScheduleReleased)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.SubscriptionScheduleUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////-------------------------------------Tax------------------------------
                    //else if (stripeEvent.Type == Events.TaxRateCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.TaxRateUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////---------------------------------------Topup----------------------------------
                    //else if (stripeEvent.Type == Events.TopupCanceled)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.TopupCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.TopupFailed)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.TopupReversed)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.TopupSucceeded)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    ////-----------------------------------------------Transfer-------------------------------------
                    //else if (stripeEvent.Type == Events.TransferCreated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.TransferReversed)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                    //else if (stripeEvent.Type == Events.TransferUpdated)
                    //{
                    //    Customer cc = stripeEvent.Data.Object as Customer;
                    //    type_id = cc.Id;
                    //}
                }
                using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
                {
                    con.Open();
                    Console.WriteLine("Inserting Web Hook 2: " + stripeevent + ", " + type + ", " + type_id);
                    string str = string.Format(@"
                        INSERT INTO 
                            eb_stripeevents (event,type,type_id,created_at,cust_id)
                        VALUES('{0}','{1}','{2}','{3}','{4}')", stripeevent, type, type_id, DateTime.Now, cust_id);
                    Console.WriteLine("Web Hook Connection  DBName : " + InfraConnectionFactory.DataDB.DBName);
                    DbCommand cmd = InfraConnectionFactory.DataDB.GetNewCommand(con, str);

                    cmd.ExecuteNonQuery();
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("Error in Webhook Handling : " + e.Message + e.StackTrace);
                //return BadRequest();
            }
        }

        public GetCustomerInvoiceResponse Post(GetCustomerInvoiceRequest request)
        {
            GetCustomerInvoiceResponse resp = new GetCustomerInvoiceResponse();
            StripeConfiguration.ApiKey = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_STRIPE_SECRET_KEY);

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
            int cnt = 0;
            List<Eb_StripeInvoice> List = new List<Eb_StripeInvoice>();
            List<Eb_StripeSubInvoice> SubList=null;
            for (int i = 0; i < count; i++)
            {
                cnt = invoices.Data[i].Lines.Data.Count;
                var invoice = service1.Get(invoices.Data[i].Id);
                for (int j = 0; j < cnt; j++)
                {
                    SubList = new List<Eb_StripeSubInvoice>();
                    SubList.Add(new Eb_StripeSubInvoice
                    {
                        PlanId = invoices.Data[i].Lines.Data[j].Plan.Id,
                        Amount = invoices.Data[i].Lines.Data[j].Plan.Amount / 100,
                        Type = invoices.Data[i].Lines.Data[j].Type,
                        Description = invoices.Data[i].Lines.Data[j].Description,
                        Currency = invoices.Data[i].Lines.Data[j].Currency,
                        Quantity = invoices.Data[i].Lines.Data[j].Quantity,
                        Total = invoices.Data[i].Lines.Data[j].Amount / 100
                    });
                }
                List.Add(new Eb_StripeInvoice
                {
                    Id = invoices.Data[i].Id,
                    Date = invoices.Data[i].Date,
                    SubTotal = invoices.Data[i].Subtotal / 100,
                    Total = invoices.Data[i].Total / 100,
                    Url = invoice.HostedInvoiceUrl,
                    InvNumber = invoice.Number,
                    Status = invoice.Paid,
                    PeriodStart = invoices.Data[i].PeriodStart,
                    PeriodEnd = invoices.Data[i].PeriodEnd,
                    Duration = invoices.Data[i].Discount == null ? 0 : invoices.Data[i].Discount.Coupon.Duration,
                    PercentOff = invoices.Data[i].Discount == null ? 0 : invoices.Data[i].Discount.Coupon.PercentOff,
                    SubList = SubList
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
                Duration = Inv.Discount == null ? 0 : Inv.Discount.Coupon.Duration,
                Data = Data
            };

            return resp;
        }

        public ChangeCardSourceResponse Post(ChangeCardSourceRequest request)
        {
            ChangeCardSourceResponse resp = new ChangeCardSourceResponse();
            StripeCustomer updatedCustomer = gateway.Post(new UpdateStripeCustomer
            {
                Id = request.CustId,
                DefaultSource = request.CardId
            });
            resp.CardId = request.CardId;
            return resp;
        }
    }


}
