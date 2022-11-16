using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;
using RestSharp.Serialization.Json;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using ResponseStatus = ServiceStack.ResponseStatus;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class ApiConversionService : EbBaseService
    {
        private ResponseStatus _Responsestatus = new ResponseStatus();
        EbDataSet ds = new EbDataSet();

        public ApiConversionResponse Any(ApiConversionRequest request)
        {
            ApiConversionResponse resp = null;
            resp = start_get(request);
            return resp;
        }

        public IRestResponse Execute(ApiConversionRequest request)
        {
            List<Param> param = (request.Parameters != null) ? request.Parameters : new List<Param>();
            string Url = (request.Method == ApiMethods.GET && param.Count > 0) ? ModifyUrl(request.Url, param) : request.Url;
            Uri uri = new Uri(Url);
            IRestResponse response = null;
            try
            {
                RestClient client = new RestClient(uri.GetLeftPart(UriPartial.Authority));
                Method Method = (request.Method == ApiMethods.POST) ? RestSharp.Method.POST : RestSharp.Method.GET;
                RestRequest rest_req = new RestRequest(uri.PathAndQuery, Method);

                if (request.Headers != null && request.Headers.Any())
                {
                    foreach (ApiRequestHeader header in request.Headers)
                    {
                        rest_req.AddHeader(header.HeaderName, header.Value);
                    }
                }

                if (uri.GetLeftPart(System.UriPartial.Authority).Contains(RoutingConstants.LIVEHOSTADDRESS) ||
                    uri.GetLeftPart(System.UriPartial.Authority).Contains(RoutingConstants.STAGEHOSTADDRESS))
                {
                    rest_req.AddHeader("bToken", this.Request.Headers["Authorization"]);
                    rest_req.AddHeader("rToken", this.Request.Headers["rToken"]);
                }
                foreach (Param _param in param)
                {
                    rest_req.AddParameter(_param.Name, _param.ValueTo);
                }
                response = client.Execute(rest_req);


                //using (var client = new HttpClient())
                //{
                //    if (request.Headers != null && request.Headers.Any())
                //    {
                //        foreach (ApiRequestHeader header in request.Headers)
                //        {
                //            client.DefaultRequestHeaders.Add(header.HeaderName, header.Value);
                //        }
                //    }

                //    client.BaseAddress = new Uri(uri.GetLeftPart(System.UriPartial.Authority));

                //    if (uri.GetLeftPart(System.UriPartial.Authority).Contains(RoutingConstants.LIVEHOSTADDRESS) ||
                //        uri.GetLeftPart(System.UriPartial.Authority).Contains(RoutingConstants.STAGEHOSTADDRESS))
                //    {
                //        client.BaseAddress.Add("bToken", this.Request.Headers["bToken"]);
                //        client.DefaultRequestHeaders.Add("rToken", this.Request.Headers["rToken"]);
                //    }
                //    if (request.Method == ApiMethods.POST)
                //    {
                //        var parameters = param.Select(i => new { prop = i.Name, val = i.Value })
                //                .ToDictionary(x => x.prop, x => x.val);
                //        response = client.PostAsync(uri.PathAndQuery, new FormUrlEncodedContent(parameters)).Result;
                //    }
                //    else if (request.Method == ApiMethods.GET)
                //    {
                //        response = client.GetAsync(uri.PathAndQuery).Result;
                //    }
                //}
            }
            catch (Exception e)
            {
                throw e;
            }
            return response;
        }

        private string ModifyUrl(string longurl, List<Param> param)
        {
            var uriBuilder = new UriBuilder(longurl);
            var query = HttpUtility.ParseQueryString(uriBuilder.Query);
            foreach (Param i in param)
                query[i.Name] = i.Value;
            //query["action"] = "login1";
            uriBuilder.Query = query.ToString();
            longurl = uriBuilder.ToString();
            return longurl;
        }

        private List<Param> GetParams(List<ApiRequestParam> Parameters)
        {
            return Parameters.Select(i => new Param { Name = i.ParamName, Type = i.Type.ToString(), Value = i.Value })
                    .ToList();
        }

        private ApiConversionResponse start_get(ApiConversionRequest request)
        {
            ApiConversionResponse resp = new ApiConversionResponse();
            IRestResponse response = null;
            try
            {
                response = Execute(request);
                string rawResponse = response.Content;
                JObject jsonObject = JObject.Parse(rawResponse);
                List<JProperty> Jproperty = null;
                Uri uri = new Uri(request.Url);
                if (uri.GetLeftPart(System.UriPartial.Authority).Contains(RoutingConstants.LIVEHOSTADDRESS) ||
                    uri.GetLeftPart(System.UriPartial.Authority).Contains(RoutingConstants.STAGEHOSTADDRESS))
                {
                    List<JProperty> Jproperty1 = jsonObject.Properties().Where(pp => pp.Name == "result").ToList();
                    try
                    {
                        JObject data = JObject.Parse(Jproperty1[0].Value.ToString());
                        Jproperty = data.Properties().Where(pp => pp.Name == "data").ToList();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message + e.StackTrace);
                        Console.WriteLine(response.IsSuccessful + "  " + response.StatusCode + "  " + response.ErrorMessage + "  " + response.Content);
                    }
                }
                else
                    Jproperty = jsonObject.Properties().Where(pp => pp.Value.Type == JTokenType.Array).ToList();
               
                if (Jproperty != null)
                    foreach (JProperty property in Jproperty)
                        GetRecursive(property);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
                Console.WriteLine(e.Message);
                this._Responsestatus.Message = e.Message;
            }
            resp.dataset = ds;
            if (response != null)
                resp.statusCode = (int)response.StatusCode;
            return resp;
        }

        public EbDbTypes ConvertToEbdbType(JTokenType _type)
        {
            if (_type == JTokenType.Integer)
                return EbDbTypes.Int32;
            else if (_type == JTokenType.Boolean)
                return EbDbTypes.Boolean;
            else if (_type == JTokenType.Float)
                return EbDbTypes.Decimal;
            else if (_type == JTokenType.Date)
                return EbDbTypes.DateTime;
            else
                return EbDbTypes.String;
        }

        public object ConvertValueToEbdbType(JTokenType _type, JToken value)
        {
            if (_type == JTokenType.Integer)
                return Convert.ToInt32(value);
            else if (_type == JTokenType.Boolean)
                return Convert.ToBoolean(value);
            else if (_type == JTokenType.Float)
                return Convert.ToDecimal(value);
            else if (_type == JTokenType.Date)
                return Convert.ToDateTime(value);
            else
                return value.ToString();
        }

        private void GetRecursive(JProperty property)
        {
            int k = 0; int i = 0;
            ds.Tables.Add(new EbDataTable(property.Name));
            JTokenType _type;
            foreach (var children in property.Value.Children())
            {
                if (i == 0)
                {
                    ds.Tables[property.Name].Rows.Add(new EbDataRow());
                    foreach (var prop in children.Children<JProperty>().ToArray())
                    {
                        if (prop.Value.Type == JTokenType.Array)
                            GetRecursive(prop);
                        else
                        {
                            if (prop.Name == "id" && Int32.TryParse((string)prop.Value, out _))
                            {
                                _type = JTokenType.Integer;
                            }
                            else
                            {
                                _type = prop.Value.Type;
                            }
                            ds.Tables[property.Name].Columns.Add(new EbDataColumn { ColumnIndex = k, ColumnName = prop.Name.Replace(" ", "_"), Type = ConvertToEbdbType(_type) });

                            ds.Tables[property.Name].Rows[i][k++] = ConvertValueToEbdbType(_type, prop.Value);
                        }
                    }
                }
                else
                {
                    ds.Tables[property.Name].Rows.Add(ds.Tables[property.Name].NewDataRow2());
                    k = 0;
                    foreach (var prop in children.Children<JProperty>().ToArray())
                    {
                        if (prop.Name == "id" && Int32.TryParse((string)prop.Value, out _))
                        {
                            _type = JTokenType.Integer;
                        }
                        else
                        {
                            _type = prop.Value.Type;
                        }
                        ds.Tables[property.Name].Rows[i][k++] = ConvertValueToEbdbType(prop.Value.Type, prop.Value);
                    }
                }
                i++;
            }
        }

        //public string ApiErrorHandling(int )
        //{

        //}
    }
}
