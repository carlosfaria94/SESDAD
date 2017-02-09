using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PuppetMasterURL
{
    public class Publicador
    {
        private string id;
        private int port;
        private string url;
        private string site;

        public Publicador(string id, int port, string url)
        {
            this.id = id;
            this.port = port;
            this.url = url;
        }

        public string ID
        {
            get
            {
                return id;
            }
            set
            {
                id = value;
            }
        }

        public string Site
        {
            get
            {
                return site;
            }
            set
            {
                site = value;
            }
        }


        public int Port
        {
            get
            {
                return port;
            }
            set
            {
                port = value;
            }
        }

        public string Url
        {
            get
            {
                return url;
            }
            set
            {
                url = value;
            }
        }
    }
}

public class Subscritor
{
    private string id;
    private int port;
    private string url;
    private string site;

    public Subscritor(string id, int port, string url)
    {
        this.id = id;
        this.port = port;
        this.url = url;
    }

    public Subscritor(string id, string url)
    {
        this.id = id;
        this.url = url;
    }

    public string ID
    {
        get
        {
            return id;
        }
        set
        {
            id = value;
        }
    }

    public int Port
    {
        get
        {
            return port;
        }
        set
        {
            port = value;
        }
    }

    public string Url
    {
        get
        {
            return url;
        }
        set
        {
            url = value;
        }
    }

    public string Site
    {
        get
        {
            return site;
        }
        set
        {
            site = value;
        }
    }
}

public class Broker
{
    private string id;
    private int port;
    private string url;
    private string site;
    private List<Tabela> tabelaEncaminhamento = new List<Tabela>();

    public Broker(string id, int port, string url)
    {
        this.id = id;
        this.port = port;
        this.url = url;
    }

    public List<Tabela> TabelaEncaminhanento
    {
        get
        {
            return tabelaEncaminhamento;
        }
    }

    public string ID
    {
        get
        {
            return id;
        }
        set
        {
            id = value;
        }
    }

    public string Site
    {
        get
        {
            return site;
        }
        set
        {
            site = value;
        }
    }


    public int Port
    {
        get
        {
            return port;
        }
        set
        {
            port = value;
        }
    }

    public string Url
    {
        get
        {
            return url;
        }
        set
        {
            url = value;
        }
    }

    public void adicionaEntradaTabela(Tabela entrada)
    {
        tabelaEncaminhamento.Add(entrada);
    }
}

public class Tabela
{
    private string idBroker;
    private string idSite;
    private string url;

    public Tabela(string idBroker, string idSite, string url)
    {
        this.idBroker = idBroker;
        this.idSite = idSite;
        this.url = url;
    }

    public string IDBroker
    {
        get
        {
            return idBroker;
        }
        set
        {
            idBroker = value;
        }
    }

    public string Site
    {
        get
        {
            return idSite;
        }
        set
        {
            idSite = value;
        }
    }

    public string Url
    {
        get
        {
            return url;
        }
        set
        {
            url = value;
        }
    }
}

