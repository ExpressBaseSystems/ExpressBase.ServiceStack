﻿using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Microsoft.IdentityModel.Tokens;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Text;
using ExpressBase.Security;
using ServiceStack.Auth;
using ExpressBase.Common.LocationNSolution;
using ExpressBase.Common.Constants;
using ExpressBase.ServiceStack.MQServices;
using System.Security.Principal;
using ServiceStack;
using ExpressBase.Common.Security;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using ExpressBase.Common.Extensions;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class TwoFactorAuthServices : EbBaseService
    {
        public TwoFactorAuthServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        public const string LoginOtpMessage = "One-Time Password for log in to {0} is {1}. Do not share with anyone. This OTP is valid for 3 minutes.";
        public const string VerificationMessage = "Your verification code for {0} is {1}";
        public MyAuthenticateResponse MyAuthenticateResponse { get; set; }

        public Authenticate2FAResponse AuthResponse { get; set; }

        public Authenticate2FAResponse Post(Authenticate2FARequest request)
        {
            AuthResponse = new Authenticate2FAResponse();
            this.MyAuthenticateResponse = request.MyAuthenticateResponse;
            AuthResponse.Is2fa = true;
            AuthResponse.AuthStatus = true;
            Eb_Solution sol_Obj = GetSolutionObject(request.SolnId);
            string otp = GenerateOTP();
            User _usr = SetUserObjFor2FA(otp); // updating otp and tokens in redis userobj
            Console.WriteLine("SetUserObjFor2FA : " + MyAuthenticateResponse.User.AuthId + "," + otp);
            AuthResponse.TwoFAToken = EbTokenGenerator.GenerateToken(MyAuthenticateResponse.User.AuthId);
            if (sol_Obj.OtpDelivery2fa != null)
            {
                OtpType OtpType = 0;
                string[] _otpmethod = sol_Obj.OtpDelivery2fa.Split(",");
                if (_otpmethod[0] == "email")
                {
                    OtpType = OtpType.Email;
                }
                else if (_otpmethod[0] == "sms")
                {
                    OtpType = OtpType.Sms;
                }

                SendOtp(sol_Obj, _usr, OtpType);
                Console.WriteLine("Sent otp : " + MyAuthenticateResponse.User.AuthId + "," + otp);
            }
            else
            {
                AuthResponse.AuthStatus = false;
                AuthResponse.ErrorMessage = "Otp delivery method not set.";
            }
            return AuthResponse;
        }

        public Authenticate2FAResponse Post(ValidateTokenRequest request)
        {
            AuthResponse = new Authenticate2FAResponse();
            AuthResponse.AuthStatus = EbTokenGenerator.ValidateToken(request.Token, request.UserAuthId);
            if (!AuthResponse.AuthStatus)
            {
                AuthResponse.ErrorMessage = "Something went wrong with token";
            }
            return AuthResponse;
        }

        public Authenticate2FAResponse Post(ResendOTP2FARequest request)
        {
            AuthResponse = new Authenticate2FAResponse();
            ResendOtpInner(request.Token, request.UserAuthId, request.SolnId);
            return AuthResponse;
        }

        public Authenticate2FAResponse Post(ResendOTPSignInRequest request)
        {
            AuthResponse = new Authenticate2FAResponse();
            ResendOtpInner(request.Token, request.UserAuthId, request.SolnId);
            return AuthResponse;
        }

        public Authenticate2FAResponse Post(SendSignInOtpRequest request)
        {
            AuthResponse = new Authenticate2FAResponse();
            try
            {
                string authColumn = (request.SignInOtpType == OtpType.Email) ? "email" : "phnoprimary";
                this.EbConnectionFactory = new EbConnectionFactory(request.SolutionId, this.Redis);
                if (EbConnectionFactory != null)
                {
                    string query = String.Format("SELECT id FROM eb_users WHERE {0} = '{1}' AND (statusid = 0 OR statusid = 4)", authColumn, request.UName);
                    EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
                    if (dt != null && dt.Rows.Count > 0)
                    {
                        Eb_Solution sol_Obj = GetSolutionObject(request.SolutionId);
                        string UserAuthId = string.Format(TokenConstants.SUB_FORMAT, request.SolutionId, dt.Rows[0][0], (!string.IsNullOrEmpty(request.WhichConsole)) ? (request.WhichConsole) : (TokenConstants.UC));
                        string otp = GenerateOTP();
                        User _usr = SetUserObjForSigninOtp(otp, UserAuthId);

                        AuthResponse.TwoFAToken = EbTokenGenerator.GenerateToken(UserAuthId);
                        SendOtp(sol_Obj, _usr, request.SignInOtpType);
                        Console.WriteLine("Sent otp : " + UserAuthId + "," + otp);

                        AuthResponse.AuthStatus = true;
                        AuthResponse.UserAuthId = UserAuthId;
                    }
                    else
                    {
                        AuthResponse.AuthStatus = false;
                        AuthResponse.ErrorMessage = "Invalid User";
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return AuthResponse;
        }

        public Authenticate2FAResponse Post(SetForgotPWInRedisRequest request)
        {
            AuthResponse = new Authenticate2FAResponse();
            try
            {
                this.EbConnectionFactory = new EbConnectionFactory(request.SolutionId, this.Redis);
                if (EbConnectionFactory != null)
                {
                    string query = String.Format("SELECT id FROM eb_users WHERE email = '{0}' OR phnoprimary = '{0}' AND (statusid = 0 OR statusid = 4) ", request.UName);
                    EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
                    if (dt != null && dt.Rows.Count > 0)
                    {
                        Eb_Solution sol_Obj = GetSolutionObject(request.SolutionId);
                        string UserAuthId = string.Format(TokenConstants.SUB_FORMAT, request.SolutionId, dt.Rows[0][0], (!string.IsNullOrEmpty(request.WhichConsole)) ? (request.WhichConsole) : (TokenConstants.UC));

                        string q2 = String.Format("UPDATE eb_users SET forcepwreset = 'T' WHERE id = {0}", dt.Rows[0][0]);
                        EbDataTable dt2 = this.EbConnectionFactory.DataDB.DoQuery(q2);
                        this.Redis.Set<bool>("Fpw_" + UserAuthId, true, new TimeSpan(1, 0, 0));

                        AuthResponse.AuthStatus = true;
                        AuthResponse.UserAuthId = UserAuthId;
                    }
                    else
                    {
                        AuthResponse.AuthStatus = false;
                        AuthResponse.ErrorMessage = "Invalid User";
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return AuthResponse;
        }

        public Authenticate2FAResponse Post(SendVerificationCodeRequest request)
        {
            Authenticate2FAResponse response = new Authenticate2FAResponse()
            {
                EmailVerifCode = new Authenticate2FAResponse(),
                MobileVerifCode = new Authenticate2FAResponse()
            };
            string subject = "Verification";
            Eb_Solution sol_Obj = GetSolutionObject(request.SolnId);
            User usr = GetUserObject(request.UserAuthId);
            if (sol_Obj == null)
                response.Message = "Solution object is null";
            else if (usr == null)
                response.Message = "User object is null";
            else
            {
                try
                {
                    if (!string.IsNullOrEmpty(request.Email))
                    {
                        string Verifcode = GenerateOTP();
                        string message = string.Format(VerificationMessage, sol_Obj.SolutionName, Verifcode);
                        usr.Email = request.Email;///
                        SendOtpEmail(usr, sol_Obj.SolutionID, message, subject);
                        response.EmailVerifCode.AuthStatus = true;
                        response.EmailVerifCode.Message = "Email verification code sent";
                        this.Redis.Set(request.Key + request.Email.RemoveSpecialCharacters(), Verifcode, new TimeSpan(0, 1, 0, 0));
                    }
                }
                catch (Exception ex)
                {
                    response.EmailVerifCode.Message = ex.Message;
                }

                try
                {
                    if (!string.IsNullOrEmpty(request.Mobile))
                    {
                        string Verifcode = GenerateOTP();
                        string message = string.Format(VerificationMessage, sol_Obj.SolutionName, Verifcode);
                        usr.PhoneNumber = request.Mobile;///
                        SendOtpSms(usr, sol_Obj.SolutionID, message);
                        response.MobileVerifCode.AuthStatus = true;
                        response.MobileVerifCode.Message = "Mobile verification code sent";
                        this.Redis.Set(request.Key + request.Mobile.RemoveSpecialCharacters(), Verifcode, new TimeSpan(0, 1, 0, 0));
                    }
                }
                catch (Exception ex)
                {
                    response.MobileVerifCode.Message = ex.Message;
                }
            }
            return response;
        }

        public Authenticate2FAResponse Post(VerifyVerificationCodeRequest request)
        {
            Authenticate2FAResponse response = new Authenticate2FAResponse()
            {
                EmailVerifCode = new Authenticate2FAResponse(),
                MobileVerifCode = new Authenticate2FAResponse(),
                Message = "Success"
            };
            try
            {
                if (!string.IsNullOrEmpty(request.Email))
                {
                    string otpInRedis = this.Redis.Get<string>(request.Key + request.Email.RemoveSpecialCharacters());
                    if (otpInRedis != null && otpInRedis == request.Otp)
                    {
                        response.EmailVerifCode.AuthStatus = true;
                        response.Message = "| Correct email otp |";
                    }
                    else
                        response.Message = "| Incorrect email otp |";
                }
                if (!string.IsNullOrEmpty(request.Mobile))
                {
                    string otpInRedis = this.Redis.Get<string>(request.Key + request.Mobile.RemoveSpecialCharacters());
                    if (otpInRedis != null && otpInRedis == request.Otp)
                    {
                        response.MobileVerifCode.AuthStatus = true;
                        response.Message += "| Correct Mobile otp |";
                    }
                    else
                        response.Message += "| Incorrect Mobile otp |";
                }
            }
            catch (Exception ex)
            {
                response.ErrorMessage = ex.Message;
            }
            return response;
        }

        public Authenticate2FAResponse Post(SendUserVerifCodeRequest request)
        {
            AuthResponse = new Authenticate2FAResponse();
            try
            {
                string uAuthId = request.SolnId + ":" + request.UserId + ":" + request.WC;
                User u = GetUserObject(uAuthId);
                if (u != null)
                {
                    Eb_Solution sol_Obj = GetSolutionObject(request.SolnId);
                    if (sol_Obj != null)
                    {
                        string subject = "Verification";
                        try
                        {
                            if (!string.IsNullOrEmpty(u.Email))
                            {
                                AuthResponse.EmailVerifCode = SendEmailVerificationCode(subject, u, sol_Obj);
                            }
                            if (!string.IsNullOrEmpty(u.PhoneNumber))
                            {
                                AuthResponse.MobileVerifCode = SendMobileVerificationCode(subject, u, sol_Obj);
                            }
                            AuthResponse.AuthStatus = true;
                        }
                        catch (Exception e)
                        {
                            AuthResponse.AuthStatus = false;
                            throw e;
                        }
                    }
                    else
                    {
                        AuthResponse.AuthStatus = false;
                        AuthResponse.ErrorMessage = "SolutionObject Is Null";
                    }
                }
                else
                {
                    AuthResponse.AuthStatus = false;
                    AuthResponse.ErrorMessage = "UserObject Is Null";
                }
            }
            catch (Exception e)
            {
                AuthResponse.AuthStatus = false;
                AuthResponse.ErrorMessage += e.Message;
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return AuthResponse;
        }

        public Authenticate2FAResponse Post(VerifyUserConfirmationRequest request)
        {
            AuthResponse = new Authenticate2FAResponse();
            User u = GetUserObject(request.UserAuthId);
            if (request.VerificationCode == u.EmailVerifCode)
            {
                AuthResponse.AuthStatus = true;
                AuthResponse.Message = "Email verification success";
                User.UpdateVerificationStatus(this.EbConnectionFactory.DataDB, u.Id, true, false);
            }
            else if (request.VerificationCode == u.MobileVerifCode)
            {
                AuthResponse.AuthStatus = true;
                AuthResponse.Message = "Mobile verification success";
                User.UpdateVerificationStatus(this.EbConnectionFactory.DataDB, u.Id, false, true);
            }
            return AuthResponse;
        }

        private void ResendOtpInner(string Token, string UserAuthId, string SolnId)
        {
            AuthResponse.AuthStatus = EbTokenGenerator.ValidateToken(Token, UserAuthId);
            if (AuthResponse.AuthStatus)
            {
                Console.WriteLine("Otp token valid");
                Eb_Solution sol_Obj = GetSolutionObject(SolnId);
                User _usr = GetUserObject(UserAuthId);
                string[] _otpmethod = sol_Obj.OtpDelivery2fa.Split(",");
                OtpType SignInOtpType = 0;
                if (_otpmethod[0] == "email")
                {
                    SignInOtpType = OtpType.Email;
                }
                else if (_otpmethod[0] == "sms")
                {
                    SignInOtpType = OtpType.Sms;
                }

                SendOtp(sol_Obj, _usr, SignInOtpType);
            }
            else
            {
                AuthResponse.ErrorMessage = "Something went wrong with token";
            }
        }

        private string GenerateOTP()
        {
            string sOTP = String.Empty;
            string sTempChars = String.Empty;
            int iOTPLength = 6;
            string[] saAllowedCharacters = { "1", "2", "3", "4", "5", "6", "7", "8", "9", "0" };
            Random rand = new Random();
            for (int i = 0; i < iOTPLength; i++)
            {
                int p = rand.Next(0, saAllowedCharacters.Length);
                sTempChars = saAllowedCharacters[rand.Next(0, saAllowedCharacters.Length)];
                sOTP += sTempChars;
            }
            return sOTP;
        }

        internal User SetUserObjFor2FA(string otp = null)
        {
            User u = GetUserObject(this.MyAuthenticateResponse.User.AuthId);
            u.Otp = otp;
            u.BearerToken = this.MyAuthenticateResponse.BearerToken;
            u.RefreshToken = this.MyAuthenticateResponse.RefreshToken;
            this.Redis.Set<IUserAuth>(this.MyAuthenticateResponse.User.AuthId, u);// must set as IUserAuth
            return u;
        }

        private User SetUserObjForSigninOtp(string otp, string UserAuthId)
        {
            User u = GetUserObject(UserAuthId, true);
            if (u != null)
            {
                Console.WriteLine("otp : " + otp);
                u.Otp = otp;
                this.Redis.Set<IUserAuth>(UserAuthId, u);// must set as IUserAuth
            }
            else
            {
                Console.WriteLine("Userobj is null :" + UserAuthId);
            }
            return u;
        }

        private User SetUserObjForMobileVerifCode(string otp, string UserAuthId)
        {
            User u = GetUserObject(UserAuthId);
            if (u != null)
            {
                Console.WriteLine("otp : " + otp);
                u.MobileVerifCode = otp;
                this.Redis.Set<IUserAuth>(UserAuthId, u);// must set as IUserAuth
            }
            else
            {
                Console.WriteLine("Userobj is null :" + UserAuthId);
            }
            return u;
        }

        private User SetUserObjForEmailVerifCode(string otp, string UserAuthId)
        {
            User u = GetUserObject(UserAuthId);
            if (u != null)
            {
                Console.WriteLine("otp : " + otp);
                u.EmailVerifCode = otp;
                this.Redis.Set<IUserAuth>(UserAuthId, u);// must set as IUserAuth
            }
            else
            {
                Console.WriteLine("Userobj is null :" + UserAuthId);
            }
            return u;
        }

        private void SendOtp(Eb_Solution sol_Obj, User _usr, OtpType OtpType)//same otp to email and phone, signil like purpose
        {
            try
            {
                string subject = "OTP Verification";
                string message = string.Format(LoginOtpMessage, sol_Obj.SolutionName, _usr.Otp);
                AuthResponse.AuthStatus = false;

                if (sol_Obj.IsEmailIntegrated)
                {
                    if (!string.IsNullOrEmpty(_usr.Email))
                    {
                        SendOtpEmail(_usr, sol_Obj.SolutionID, message, subject);
                        AuthResponse.AuthStatus = true;

                        int end = _usr.Email.IndexOf('@');
                        if (end > 0)
                        {
                            string name = _usr.Email.Substring(3, end - 3);
                            string newString = new string('*', name.Length);
                            string final = _usr.Email.Replace(name, " " + newString);
                            AuthResponse.OtpTo += final;
                        }
                    }
                    else
                    {
                        AuthResponse.ErrorMessage = "Email id not set for the user. Please contact your admin";
                    }
                }
                if (sol_Obj.IsSmsIntegrated)
                {
                    if (!string.IsNullOrEmpty(_usr.PhoneNumber))
                    {
                        string lastDigit = _usr.PhoneNumber.Substring((_usr.PhoneNumber.Length - 4), 4);
                        SendOtpSms(_usr, sol_Obj.SolutionID, message);
                        AuthResponse.AuthStatus = true;
                        AuthResponse.OtpTo += ", ******" + lastDigit;
                    }
                    else
                    {
                        AuthResponse.ErrorMessage = "Phone number not set for the user. Please contact your admin";
                    }
                }
            }
            catch (Exception e)
            {
                AuthResponse.AuthStatus = false;
                AuthResponse.ErrorMessage = e.Message;
            }
        }

        private void SendOtpEmail(User _usr, string solnId, string message, string subject)
        {
            EmailService emailService = base.ResolveService<EmailService>();
            emailService.Post(new EmailDirectRequest
            {
                To = _usr.Email,
                Subject = subject,
                Message = message,
                SolnId = solnId,
                UserId = _usr.UserId,
                WhichConsole = TokenConstants.UC,
                UserAuthId = _usr.AuthId
            });
        }

        private void SendOtpSms(User _usr, string solnId, string message)
        {
            SmsCreateService smsCreateService = base.ResolveService<SmsCreateService>();
            smsCreateService.Post(new SmsDirectRequest
            {
                To = _usr.PhoneNumber,
                Body = message,
                SolnId = solnId,
                UserId = _usr.UserId,
                WhichConsole = TokenConstants.UC,
                UserAuthId = _usr.AuthId
            });
        }

        private Authenticate2FAResponse SendEmailVerificationCode(string subject, User usr, Eb_Solution soln)
        {
            Authenticate2FAResponse response = new Authenticate2FAResponse();
            try
            {
                string Verifcode = GenerateOTP();
                SetUserObjForEmailVerifCode(Verifcode, usr.AuthId);
                string message = string.Format(VerificationMessage, soln.SolutionName, Verifcode);
                SendOtpEmail(usr, soln.SolutionID, message, subject);
                response.AuthStatus = true;
                response.Message = "Email verification code sent";
            }
            catch (Exception e)
            {
                response.AuthStatus = false;
                response.ErrorMessage = e.Message;
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return response;
        }

        private Authenticate2FAResponse SendMobileVerificationCode(string subject, User usr, Eb_Solution soln)
        {
            Authenticate2FAResponse response = new Authenticate2FAResponse();
            try
            {
                string Verifcode = GenerateOTP();
                SetUserObjForMobileVerifCode(Verifcode, usr.AuthId);
                string message = string.Format(VerificationMessage, soln.SolutionName, Verifcode);
                SendOtpSms(usr, soln.SolutionID, message);
                response.AuthStatus = true;
                response.Message = "Mobile verification code sent";
            }
            catch (Exception e)
            {
                response.AuthStatus = false;
                response.ErrorMessage = e.Message;
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return response;
        }

        //for phone control
        public GetOTPResponse Post(GetOTPRequest request)
        {
            GetOTPResponse res = new GetOTPResponse();
            try
            {
                res.OTP = GenerateOTP();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return res;
        }
    }
}
