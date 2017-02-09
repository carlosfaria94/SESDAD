using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RemoteObjects
{
    public interface IPuppet
    {
        void recebePubLog(string id, string topic, string eventNumber);
        void recebeSubLog(string idSubscriber, string idPublisher, string topic, string eventNumber);
        void recebeBroLog(string idBroker, string idPublisher, string topic, string eventNumber, string idSubscriber);
    }

    public interface IPuppetMasterURL
    {
        void invocaProcessoBroker(string id, string url, string urlBrokerPai, int numeroBrokers, string routingPolicy, string orderingPolicy, string loggingLevel, string urlPuppetRemoto, string urlIrmao1, string urlIrmao2, string site, bool criaTabela);
        void invocaProcessoPublisher(string id, string url, string urlBroker, string urlBroker2, string urlBroker3, string urlPuppetRemoto);
        void invocaProcessoSubscriber(string id, string url, string urlBroker, string urlBroker2, string urlBroker3, string urlPuppetRemoto);
        void recebePubLog(string id, string topic, string eventNumber);
        void recebeSubLog(string idSubscriber, string idPublisher, string topic, string eventNumber);
        void recebeBroLog(string idBroker, string idPublisher, string topic, string eventNumber);
    }

    public interface IPublisher
    {
        void recebeComando(int numeroPublicacoes, Evento evento, int intervaloEntrePublicacoes, bool freeze);
        void recebeStatus();
        string getID();
        int getPort();
        string getUrl();
        string getUrlBroker();
        void recebeACK(int iden);
        void atualizaLider(string urlPrincipal, string urlSuspeito);
        void atualizaEstadoBroker(string urlBroker, string estado);
    }

    public interface IBroker
    {
        void registoPublisher(string url);
        void registoSubscriber(string id, string url, string comando, Evento evento, string urlQuemEnviouEvento);
        void registoBroker(string url, string estado, string site);
        void unsubscribe(string id, string url, string comando, Evento evento, string urlQuemEnviouEvento);
        void recebeEvento(Evento evento, bool freeze, string urlFilho);
        void recebeStatus();
        void criarTabelaEncaminhamento(string urlPai, int numeroBrokers);
        Dictionary<string, string> getTabelaEncaminhamento();
        List<BrokerFilho> getListaFilhos();
        void propagacaoEvento(Evento evento, string idBrokerDestinoFinal, string urlSubscriber, string idSubscriber, Subscricao subscricao, string urlQuemEnviou);
        void propagacaoEventoFIFO(Evento evento, string idBrokerDestinoFinal, string urlSubscriber, string idSubscriber, Subscricao subscricao, string urlQuemEnviou);
        string getUrlPai();
        string getUrl();
        string getID();
        string getSite();
        void setRoutingPolicy(string policy, bool criaTabela);
        void setOrderingPolicy(string policy);
        string getRoutingPolicy();
        void recebeListaIrmaos(string urlIrmao1, string urlIrmao2, string estado);
        void addListaSubscriber(string url);
        void detetaSuspeito(string urlBrokerSuspeito, string urlQuemEnviou, string novoBrokerPrincipal);
        void atualizaEstado(string estado, string urlPrincipal);
        void ping(string url);
        void recebeAck(string urlSuspeito);
        void recebeCrash();
        Dictionary<string, string> getListaIrmaos();
        void AtualizaListaFilhos(List<BrokerFilho> listaFilhos);
        string getEstado();
        void recebeAckBrokers(int iden, string site, string nomePublisher);
        void atualizaURLPai(string urlPaiNovo);
        void brokerDetetaSuspeito(string urlBrokerSuspeito, string novoLider, string urlQuemEnviou);
        void atualizaEstadoBroker(string urlBroker, string estado);
        void atualizaEstadoBrokerPai(string urlNovoPai, string urlBrokerSuspeito, string estado);
        void atualizaEstadoBrokerPaiVindoFilho(string urlNovoFilho, string urlBrokerSuspeito, string estado);
        void irmaoAtualizaBrokerPai(string novoUrl, string urlBrokerSuspeito, string estado);
        void adicionaTabelaEncaminhamento(Dictionary<string, string> tabela);
        void atualizaTabelaEncaminhamento(string idBroker, string urlBroker, string urlBrokerSuspeito);
        void recebeACKRegistoSubscriber(int iden, string idSubscriber);
        void recebeACKUnsubscribe(int iden, string idSubscriber);
        void atualizaListaSubscriber(Subscricao subs, string urlQuemEnviouEvento);
        void recebeAckPropagacao(int iden, string site, string nomePublisher);
        string getIDOriginal();
        void atualizaListaSubscriber2(Evento evento, string urlQuemEnviouEvento);
        void estouVivo(string urlQuemEnviou);
        void recebeACKPedidoSequencer(int iden, string site, string nomePublisher);
        void recebeACKRecebeEventoTotal(int iden, string site, string nomePublisher);
        void recebeACKPropagacaoTotal(int iden, string site, string nomePublisher);

        void pedidoSequencer(Pedido pedido, string urlQuemEnviou);
        void recebeEventoTotal(Evento evento, string urlQuemEnviouEvento);
        void propagacaoEventoTOTAL(Evento evento, string idBrokerDestinoFinal, string urlSubscriber, string idSubscriber, Subscricao subscricao, string urlQuemEnviou);

    }

    public interface ISubscriber
    {
        void recebeComando(string comando, Evento evento, bool freeze);
        void recebeStatus();
        void recebeEvento(Evento evento, bool freeze);
        string getID();
        string getUrl();
        string getUrlBroker();
        void recebeACK(int iden);
        void atualizaLider(string urlPrincipal, string urlSuspeito);
        void atualizaEstadoBroker(string urlBroker, string estado);
        string getBrokerEmEspera(string urlBrokerSuspeito);
        void recebeACK2(int iden);

        int getSequencer();
        void incrementSequencer();
    }

    [Serializable]
    public class Evento
    {
        private string topic;
        private string[] content = new string[2]; //em content[0] vai ficar o nome do publisher e em content[1] o Sequence number do evento
        private string urlOrigem;
        private int seqNumberSite;

        //Parametros novos
        private int sequencer;
        private string subFinal;
        private string urlBrokerEnviou;


        //Parametros para a fila de espera de propagaçao
        private string idBrokerFinal;
        private string urlSubscriber;
        private string idSubscriber;
        private Subscricao subscricao;

        private string quemEnviouEvento;
        private int identificadorACK;
        private string comando; //para o caso de ser necessario reenviar algo de um subscriber, isto indica qual o comando
        private string site;


        public Evento(string topic)
        {
            this.topic = topic;
        }

        public string[] getContent()
        {
            return content;
        }

        public void setContent(string[] conteudo)
        {
            content = conteudo;
        }

        public string getTopic()
        {
            return topic;
        }

        public void setIdentificador(int iden)
        {
            identificadorACK = iden;
        }

        public void setSite(string site)
        {
            this.site = site;
        }

        public string getSite()
        {
            return site;
        }

        public void setComando(string cmd)
        {
            comando = cmd;
        }

        public string getComando()
        {
            return comando;
        }

        public int getIdentificador()
        {
            return identificadorACK;
        }

        public void setContent(string nomePublisher, int seqNumber)
        {
            content[0] = nomePublisher;
            content[1] = seqNumber.ToString();
        }

        public string getNomePublisher()
        {
            return content[0];
        }

        public int getSeqNumber()
        {
            return Int32.Parse(content[1]);
        }

        public void setUrlOrigem(string urlOrigem)
        {
            this.urlOrigem = urlOrigem;
        }

        public string getUrlOrigem()
        {
            return urlOrigem;
        }
        public string getQuemEnviouEvento()
        {
            return quemEnviouEvento;
        }

        public void setQuemEnviouEvento(string tipo)
        {
            quemEnviouEvento = tipo;
        }

        public int getSeqNumberSite()
        {
            return seqNumberSite;
        }

        public void setSeqNumberSite(int seqNumberSite)
        {
            this.seqNumberSite = seqNumberSite;
        }

        public void setIdBrokerFinal(string idBrokerFinal)
        {
            this.idBrokerFinal = idBrokerFinal;
        }

        public string getIdBrokerFinal()
        {
            return idBrokerFinal;
        }

        public void setUrlSubscriber(string urlSubscriber)
        {
            this.urlSubscriber = urlSubscriber;
        }

        public string getUrlSubscriber()
        {
            return urlSubscriber;
        }

        public void setIdSubscriber(string idSubscriber)
        {
            this.idSubscriber = idSubscriber;
        }

        public string getIdSubscriber()
        {
            return idSubscriber;
        }

        public void setSubscricao(Subscricao subscricao)
        {
            this.subscricao = subscricao;
        }

        public Subscricao getSubscricao()
        {
            return subscricao;
        }

        public void setSequencer(int sequencer)
        {
            this.sequencer = sequencer;
        }

        public int getSequencer()
        {
            return sequencer;
        }

        public void setUrlBrokerEnviou(string urlBrokerEnviou)
        {
            this.urlBrokerEnviou = urlBrokerEnviou;
        }

        public string getUrlBrokerEnviou()
        {
            return urlBrokerEnviou;
        }

        public string getSubFinal()
        {
            return subFinal;
        }

        public void setSubFinal(string subFinal)
        {
            this.subFinal = subFinal;
        }
    }

    [Serializable]
    public class Site
    {
        private string site;
        private int seqNumber;

        public Site(string site, int seqNumber)
        {
            this.site = site;
            this.seqNumber = seqNumber;
        }

        public string getSite()
        {
            return site;
        }

        public int getSeqNumber()
        {
            return seqNumber;
        }

        public void setSeqNumber(int seqNumber)
        {
            this.seqNumber = seqNumber;
        }

        public void incrementSeqNumber()
        {
            seqNumber++;
        }
    }

    [Serializable]
    public class Subscricao
    {
        private string idSubscritor;
        private Evento eventoSubscrito;
        private string url;

        public Subscricao(string idSubscritor, Evento eventoSubscrito, string url)
        {
            this.idSubscritor = idSubscritor;
            this.eventoSubscrito = eventoSubscrito;
            this.url = url;
        }

        public string getIDSubscriber()
        {
            return idSubscritor;
        }

        public string getURL()
        {
            return url;
        }

        public Evento getEvento()
        {
            return eventoSubscrito;
        }


    }

    [Serializable]
    public class BrokerFilho
    {
        private Dictionary<string, string> listaBrokers = new Dictionary<string, string>();
        private string site;

        public BrokerFilho(string url, string estado, string site)
        {
            this.site = site;
            listaBrokers.Add(url, estado);
        }

        public bool existeAtivo()
        {
            if (listaBrokers.Any(x => x.Value.Equals("ATIVO")))
                return true;
            else
                return false;
        }

        public string getBroker()
        {
            KeyValuePair<string, string> urlBrokerPrincipal = listaBrokers.First(x => x.Value.Equals("ATIVO"));
            return urlBrokerPrincipal.Key;
        }

        public void adicionaEntradaLista(string url, string estado)
        {
            listaBrokers.Add(url, estado);
        }

        public int getCountLista()
        {
            return listaBrokers.Count;
        }

        public Dictionary<string,string> getListaBrokers()
        {
            return listaBrokers;
        }

        public void setPrincipalComoSuspeito()
        {
            listaBrokers[getBroker()] = "SUSPEITO";
        }

        public void setEstado(string urlBroker, string estado)
        {
            listaBrokers[urlBroker] = estado;
        }

        public string novoLider(string urlSuspeito)
        {
         
            string aux = "";
            int auxT = 0;
            if (listaBrokers.Any(x => x.Value.Equals("ATIVO") && !x.Key.Equals(urlSuspeito)))
            {
                //se o que estiver ATIVO nao é o broker suspeito, entao é que alguem já o atualizou
                KeyValuePair<string, string> aux1 = listaBrokers.First(x => x.Value.Equals("ATIVO") && !x.Key.Equals(urlSuspeito));
                aux = aux1.Key;
            }
            else
            {
                foreach (KeyValuePair<string, string> item in listaBrokers)
                {
                    if (item.Value.Equals("ESPERA"))
                    {
                        string[] url = item.Key.Split(':'); //separamos para podermos obter o porto para inicializar o canal 
                        string[] port = url[2].Split('/'); //temos que voltar a separar para obter finalmente o porto que se encontra em port[0]
                        string[] portAux = port[0].Split('/');
                        int portFinal = Int32.Parse(portAux[0]);
                        int auxM = portFinal;
                        if (auxT == 0)
                        {
                            aux = item.Key;
                            auxT = portFinal;
                        }
                        if (auxM < auxT)
                        {
                            aux = item.Key;
                            auxT = portFinal;
                        }
                    }
                }

                listaBrokers[aux] = "ATIVO";
            }
            

            return aux;
        }

        public void setLider(string urlNovoLider)
        {
            listaBrokers[urlNovoLider] = "ATIVO";
        }

        public string getAtivo()
        {
            KeyValuePair<string, string> aux1 = listaBrokers.First(x => x.Value.Equals("ATIVO"));
            return aux1.Key;
        }

        public string getSite()
        {
            return site;
        }
    }

    [Serializable]
    public class Pedido
    {
        private string urlOrigem;
        private int sequencer;
        private int numeroPedido;
        private string idBrokerOrigem;

        //usadas para o fault tolerance
        private int seqNumber;
        private string site;
        private string nomePublisher;

        Dictionary<string, int> subscritores = new Dictionary<string, int>();

        public Pedido(string urlOrigem, int sequencer, int numeroPedido, string idBrokerOrigem)
        {
            this.urlOrigem = urlOrigem;
            this.sequencer = sequencer;
            this.numeroPedido = numeroPedido;
            this.idBrokerOrigem = idBrokerOrigem;
        }

        public string getUrlOrigem()
        {
            return urlOrigem;
        }

        public void setSequencer(int sequencer)
        {
            this.sequencer = sequencer;
        }

        public int getSequencer()
        {
            return sequencer;
        }

        public void setNumeroPedido(int numeroPedido)
        {
            this.numeroPedido = numeroPedido;
        }

        public int getNumeroPedido()
        {
            return numeroPedido;
        }

        public void incrementNumeroPedido()
        {
            numeroPedido++;
        }

        public void setIdBrokerOrigem(string idBrokerOrigem)
        {
            this.idBrokerOrigem = idBrokerOrigem;
        }

        public string getIdBrokerOrigem()
        {
            return idBrokerOrigem;
        }

        public void addSubscritor(string idSubscriber, int numero)
        {
            subscritores.Add(idSubscriber, numero);
        }

        public Dictionary<string, int> getSubscritores()
        {
            return subscritores;
        }

        public void setSite(string site)
        {
            this.site = site;
        }

        public string getSite()
        {
            return site;
        }

        public void setSeqNumber(int seq)
        {
            seqNumber = seq;
        }

        public int getSeqNumber()
        {
            return seqNumber;
        }

        public void setPublisher(string nome)
        {
            nomePublisher = nome;
        }

        public string getNomePublisher()
        {
            return nomePublisher;
        }
    }
}
