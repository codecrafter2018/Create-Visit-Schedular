using Microsoft.Xrm.Tooling.Connector;

namespace Schedular_for_visit
{
    internal class connectCrm
    {
        public CrmServiceClient connect()
        {

            var url = "YOUR-ORG-URL";
            var userName = "YOUR-ENV-USERNAME";
            var password = "YOUR-ENV-PASSWORD";

            string conn = $@"  Url = {url}; AuthType = OAuth;
                UserName = {userName};
                Password = {password};
                AppId = YOUR-APP-ID;
                RedirectUri = app://YOUR-REDIRECTID;
                LoginPrompt=Auto;
                RequireNewInstance = True";


            var svc = new CrmServiceClient(conn);
            return svc;
        }
    }
}
