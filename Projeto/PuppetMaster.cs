using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;
using RemoteObjects;
using System.Diagnostics;
using System.Runtime.Remoting.Messaging;
using System.Threading;
using System.IO;

namespace Projeto
{
    class PuppetMaster
    {
        static List<Broker> listaBrokers = new List<Broker>();
        static List<Publicador> listaPublishers = new List<Publicador>();
        static List<Subscritor> listaSubscribers = new List<Subscritor>();

        static public Object lock_ = new Object();

        static List<Arvore> listaNodes = new List<Arvore>(); //lista com a estrutura do sistema

        public delegate void delFuncaoAssincronaSubscriber(string comando, Evento evento, bool freeze);
        public delegate void delSubscriberEvento(Evento evento, bool freeze);
        public delegate void delBroker(Evento evento, bool freeze, string urlFilho);
        public delegate void delFuncaoAssincronaPublisher(int numeroPublicacoes, Evento evento, int intervaloEntrePublicacoes, bool freeze);
        public delegate void delStatus();
        public delegate void delTabela(string urlPai, int numeroBrokers);
        public delegate void delPolicies(string policy);
        public delegate void delPolicies2(string policy, bool criaTabela);
        public delegate void delEnviaIrmaos(List<string> listaIrmaos);

        private static string routingPolicy;
        private static string orderingPolicy;
        public static string loggingLevel = "light"; //default é light

        static AsyncCallback funcaoCallBack; //irá chamar uma função quando a função assincrona terminar
        static IAsyncResult ar;

        static void Main(string[] args)
        {
            TcpChannel channel = new TcpChannel(8999);
            ChannelServices.RegisterChannel(channel, false);
            RemotingConfiguration.RegisterWellKnownServiceType(typeof(puppetObject), "PuppetMasterObject", WellKnownObjectMode.Singleton);

            string urlPuppet = "tcp://localhost:8999/PuppetMasterObject";

            lerFicheiroConfiguracao(urlPuppet); //função que inicializa a rede e os processos
            enviaPoliticas(); //enviamos as politicas para os processos
            lerScript(); //leitura do ficheiro script com os comandos a executar

            while (true)
            {
                string comando = Console.ReadLine();
                executaComando(comando);
            }
        }

        public static void lerFicheiroConfiguracao(string urlPuppet)
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 18);
            string siteAnterior = "";

            string[] linhasFicheiroConfiguracao = File.ReadAllLines(path + "\\ficheiroConfiguracao.txt"); //ler o ficheiro de configuração

            //ler cada linha do ficheiro de configuração e inicializar o sistema
            foreach (string linha in linhasFicheiroConfiguracao)
            {
                string[] separador = linha.Split(' ');


                if (separador[0].Equals("Site")) //Dentro deste if, é criado a estrutura da rede
                {
                    if (separador[3].Equals("none")) //se nao tiver pai, então é a raiz da rede
                    {
                        Arvore raiz = new Arvore(separador[1]); //separador[1]= nome do site
                        listaNodes.Add(raiz);
                    }
                    else //se nao for a raiz a ser criada, temos que iterar sobre todos os nós da rede para ver quem é o pai e adiciona-lo como filho
                    {
                        for (int i = 0; i < listaNodes.Count; i++)
                        {
                            if (separador[3].Equals(listaNodes[i].Name)) //separador[3] indica quem é o pai
                            {
                                Arvore no = new Arvore(separador[1]); //separador[1] = id do site
                                listaNodes[i].AddChild(no); //adicionamos à raiz, o respetivo filho
                                listaNodes.Add(no); //adicionamos o pai à lista de nós
                            }
                        }
                    }
                }
                else if (separador[0].Equals("Process")) //se true, criamos o processo para a nova entidade
                {
                    string[] local = separador[7].Split('/');

                    string[] localFinal = local[2].Split(':');
                    string urlPuppetRemoto = "tcp://" + localFinal[0] + ":10000/PuppetMasterObjectURL";

                    criaListas(separador); //adiciona à lista de brokers/subscribers/publishers o respetivo
                    switch (separador[3]) //[3] Broker,Subscriber ou Publisher
                    {
                        case "broker":
                            //vamos à arvore dos sites saber quem é o pai deste site, se existir(no caso de nao ser a raiz), entao passamos o seu url como argumento ao novo broker criado
                            Arvore pai = listaNodes.Find(x => x.Name == separador[5]).Parent; //[5] = nome/id do site (site0, site1, etc)
                            Arvore siteAtual = listaNodes.Find(x => x.Name == separador[5]);
                       
                            siteAtual.addIrmao(separador[7]); //[7] url
                            
                            //depois de sabermos quem é o pai, através do seu nome(nome do site, i.e, site0, site1, etc), vamos buscar o seu url à listaBrokers
                            if (pai != null)
                            {
                                if (localFinal[0].Equals("localhost"))
                                {
                                    string urlBrokerPai = listaBrokers.Find(x => x.Site == pai.Name).Url;

                                    if (siteAtual.listaIrmaos.Count == 3) //se a lista de irmaos ja estiver completa, enviamos para os brokers a lista dos seus irmaos
                                    {
                                        string urlIrmao1 = siteAtual.listaIrmaos[0];
                                        string urlIrmao2 = siteAtual.listaIrmaos[1];
                                                  
                                        Process.Start(path + @"\\Broker\\bin\\Debug\\Broker.exe", separador[1] + " " + separador[7] + " " + urlBrokerPai + " " + listaNodes.Count + " " + urlPuppet + " " + loggingLevel + " " +  urlIrmao1 + " " + urlIrmao2 + " " + separador[5]); //[1] = id, [7] = url //[5]= site
                                    }
                                    else
                                        Process.Start(path + @"\\Broker\\bin\\Debug\\Broker.exe", separador[1] + " " + separador[7] + " " + urlBrokerPai + " " + listaNodes.Count + " " + urlPuppet + " " + loggingLevel + " " + "nope" + " " + "nope" +  " " + separador[5]); //[1] = id, [7] = url
                                }
                                else // se nao for localhost temos que enviar para o Puppet Master remoto
                                {
                                    string urlBrokerPai = listaBrokers.Find(x => x.Site == pai.Name).Url;

                                    IPuppetMasterURL pmRemoto = (IPuppetMasterURL)Activator.GetObject(typeof(IPuppetMasterURL), urlPuppetRemoto); //[0] -> endereço a se ligar; [1] -> porto
                                    if (siteAtual.listaIrmaos.Count == 3) //se a lista de irmaos ja estiver completa, enviamos para os brokers a lista dos seus irmaos
                                    {
                                        string urlIrmao1 = siteAtual.listaIrmaos[0];
                                        string urlIrmao2 = siteAtual.listaIrmaos[1];
                                        pmRemoto.invocaProcessoBroker(separador[1], separador[7], urlBrokerPai, listaNodes.Count, routingPolicy, orderingPolicy, loggingLevel, urlPuppet, urlIrmao1, urlIrmao2, separador[5], false); //[1] = id [7] = url //[5]= site
                                    }
                                    else
                                    {
                                        if (siteAtual.listaIrmaos.Count == 1) //só os brokers principais criam a tabela de encaminhamento. Identificamos isto através do argumento do setRoutingPolicy que se for true cria a tabela, caso contrario apenas faz set da routing policy                       
                                            pmRemoto.invocaProcessoBroker(separador[1], separador[7], urlBrokerPai, listaNodes.Count, routingPolicy, orderingPolicy, loggingLevel, urlPuppet, "nope", "nope", separador[5],true); //[1] = id [7] = url
                                        else
                                            pmRemoto.invocaProcessoBroker(separador[1], separador[7], urlBrokerPai, listaNodes.Count, routingPolicy, orderingPolicy, loggingLevel, urlPuppet, "nope", "nope", separador[5], false); //[1] = id [7] = url
                                    }
                                }
                            }
                            else
                            {
                                if (localFinal[0].Equals("localhost"))
                                    if (siteAtual.listaIrmaos.Count == 3) //se a lista de irmaos ja estiver completa, enviamos para os brokers a lista dos seus irmaos
                                    {
                                        string urlIrmao1 = siteAtual.listaIrmaos[0];
                                        string urlIrmao2 = siteAtual.listaIrmaos[1];
                                        Process.Start(path + @"\\Broker\\bin\\Debug\\Broker.exe", separador[1] + " " + separador[7] + " " + "raiz" + " " + listaNodes.Count + " " + urlPuppet + " " + loggingLevel + " " + urlIrmao1 + " " + urlIrmao2 + " " + separador[5]); //[1] = id, [7] = url [5] = site                   
                                    }
                                    else
                                        Process.Start(path + @"\\Broker\\bin\\Debug\\Broker.exe", separador[1] + " " + separador[7] + " " + "raiz" + " " + listaNodes.Count + " " + urlPuppet + " " + loggingLevel + " " + "nope" + " " + "nope" + " " + separador[5]); //[1] = id, [7] = url  [5] = site                   
                                else // se nao for localhost enviamos para o PM remoto
                                {
                                    IPuppetMasterURL pmRemoto = (IPuppetMasterURL)Activator.GetObject(typeof(IPuppetMasterURL), urlPuppetRemoto); //[0] -> endereço a se ligar; [1] -> porto

                                    if (siteAtual.listaIrmaos.Count == 3) //se a lista de irmaos ja estiver completa, enviamos para os brokers a lista dos seus irmaos
                                    {
                                        string urlIrmao1 = siteAtual.listaIrmaos[0];
                                        string urlIrmao2 = siteAtual.listaIrmaos[1];
                                        pmRemoto.invocaProcessoBroker(separador[1], separador[7], "raiz", listaNodes.Count, routingPolicy, orderingPolicy, loggingLevel, urlPuppet, urlIrmao1, urlIrmao2, separador[5],false); //[1] = id [7] = url [5] = site
                                    }
                                    else
                                    {
                                        if (siteAtual.listaIrmaos.Count == 1) //só os brokers principais criam a tabela de encaminhamento. Identificamos isto através do argumento do setRoutingPolicy que se for true cria a tabela, caso contrario apenas faz set da routing policy                       
                                            pmRemoto.invocaProcessoBroker(separador[1], separador[7], "raiz", listaNodes.Count, routingPolicy, orderingPolicy, loggingLevel, urlPuppet, "nope", "nope", separador[5],true); //[1] = id [7] = url [5]= site
                                        else
                                            pmRemoto.invocaProcessoBroker(separador[1], separador[7], "raiz", listaNodes.Count, routingPolicy, orderingPolicy, loggingLevel, urlPuppet, "nope", "nope", separador[5],false); //[1] = id [7] = url [5]= site
                                    }
                                }
                            }
                            break;
                        case "publisher":
                            Arvore siteAtualPublisher = listaNodes.Find(x => x.Name == separador[5]); //vamos buscar o site do publisher para posteriormente associar-lhe aos respetivos brokers 
                            if (localFinal[0].Equals("localhost"))
                                Process.Start(path + @"\\Publisher\\bin\\Debug\\Publisher.exe", separador[1] + " " + separador[7] + " " + siteAtualPublisher.listaIrmaos[0] + " " + siteAtualPublisher.listaIrmaos[1] + " " + siteAtualPublisher.listaIrmaos[2] + " " + urlPuppet); //[1] = id [7] = url                            
                            else // se nao for localhost enviamos para o PM remoto
                            {
                                IPuppetMasterURL pmRemoto = (IPuppetMasterURL)Activator.GetObject(typeof(IPuppetMasterURL), urlPuppetRemoto); //[0] -> endereço a se ligar; [1] -> porto
                                pmRemoto.invocaProcessoPublisher(separador[1], separador[7], siteAtualPublisher.listaIrmaos[0], siteAtualPublisher.listaIrmaos[1], siteAtualPublisher.listaIrmaos[2], urlPuppet);
                            }
                            break;
                        case "subscriber":
                            Arvore siteAtualSubscriber = listaNodes.Find(x => x.Name == separador[5]); //vamos buscar o site do subscriber para posteriormente associar-lhe aos respetivos brokers 
                            if (localFinal[0].Equals("localhost"))
                                Process.Start(path + @"\\Subscriber\\bin\\Debug\\Subscriber.exe", separador[1] + " " + separador[7] + " " + siteAtualSubscriber.listaIrmaos[0] + " " + siteAtualSubscriber.listaIrmaos[1] + " " + siteAtualSubscriber.listaIrmaos[2] + " " + urlPuppet); //[1] = id, [7] = url                                                       
                            else // se nao for localhost enviamos para o PM remoto      
                            {
                                IPuppetMasterURL pmRemoto = (IPuppetMasterURL)Activator.GetObject(typeof(IPuppetMasterURL), urlPuppetRemoto); //[0] -> endereço a se ligar; [1] -> porto
                                pmRemoto.invocaProcessoSubscriber(separador[1], separador[7], siteAtualSubscriber.listaIrmaos[0], siteAtualSubscriber.listaIrmaos[1], siteAtualSubscriber.listaIrmaos[2], urlPuppet);                          
                            }
                            break;
                    }
                }
                else if (separador[0].Equals("RoutingPolicy"))
                    routingPolicy = separador[1];
                else if (separador[0].Equals("OrderingPolicy"))
                    orderingPolicy = separador[1];
                else if (separador[0].Equals("LoggingLevel"))
                    loggingLevel = separador[1];
            }
        }

        public static void enviaPoliticas()
        {
            string siteAnterior = "";
            foreach (Broker bro in listaBrokers) //enviamos a routing policy para cada broker
            {
                IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), bro.Url);
                if (!siteAnterior.Equals(bro.Site)) //só os brokers principais criam a tabela de encaminhamento. Identificamos isto através do argumento do setRoutingPolicy que se for true cria a tabela, caso contrario apenas faz set da routing policy
                {
                    siteAnterior = bro.Site;
                    funcaoCallBack = new AsyncCallback(OnExitPublisher); //função que é invocada quando o método remoto assincrono retorna(OnExit)
                    delPolicies2 del = new delPolicies2(broker.setRoutingPolicy);
                    ar = del.BeginInvoke(routingPolicy, true, funcaoCallBack, null); //[1] = routing policy                
                }
                else
                {
                    funcaoCallBack = new AsyncCallback(OnExitPublisher); //função que é invocada quando o método remoto assincrono retorna(OnExit)
                    delPolicies2 del = new delPolicies2(broker.setRoutingPolicy);
                    ar = del.BeginInvoke(routingPolicy, false, funcaoCallBack, null); //[1] = routing policy
                }
                funcaoCallBack = new AsyncCallback(OnExitPublisher); //função que é invocada quando o método remoto assincrono retorna(OnExit)
                delPolicies del1 = new delPolicies(broker.setOrderingPolicy);
                ar = del1.BeginInvoke(orderingPolicy, funcaoCallBack, null); //[1] = routing policy
            }
        }

        public static void lerScript()
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 18);

            string[] linhasScript = File.ReadAllLines(path + "\\scriptComandos.txt"); //ler o ficheiro de script de comandos a executar

            foreach (string linha in linhasScript)
            {
                executaComando(linha);
            }
        }

        public static void criaListas(string[] linhaFicheiroConf)
        {
            //processo para obter o porto através do URL
            string[] url = linhaFicheiroConf[7].Split(':');
            string[] port = url[2].Split('/');
            string[] portAux = port[0].Split('/');
            int portFinal = Int32.Parse(portAux[0]);

            switch (linhaFicheiroConf[3]) //separador[3]=/Publisher,Subscriber ou Broker
            {
                case "publisher":
                    Publicador pub = new Publicador(linhaFicheiroConf[1], portFinal, linhaFicheiroConf[7], linhaFicheiroConf[5]); // separador[1] = id da nova entidade 
                    listaPublishers.Add(pub);                                                             //separador[7] url da entidade //[5] site a que pertence
                    break;
                case "subscriber":
                    Subscritor sub = new Subscritor(linhaFicheiroConf[1], portFinal, linhaFicheiroConf[7], linhaFicheiroConf[5]);
                    listaSubscribers.Add(sub);
                    break;
                case "broker":
                    Broker bro = new Broker(linhaFicheiroConf[1], portFinal, linhaFicheiroConf[7], linhaFicheiroConf[5]); //[5] = site do broker
                    listaBrokers.Add(bro);
                    break;
            }
        }

        public static void executaComando(string comando)
        {
            string[] separador = comando.Split(' ');

            if (separador[0].Equals("Subscriber")) //se for um comando para o subscriber
            {
                Subscritor sub = listaSubscribers.Find(x => x.ID == separador[1]); //para enviar a mensagem para o subscriber especificado no comando introduzido
                                                                                   //[1] id do subs
                Evento evento = new Evento(separador[3]); //[3] nome do topico
                ISubscriber obj = (ISubscriber)Activator.GetObject(typeof(ISubscriber), sub.Url);
                funcaoCallBack = new AsyncCallback(OnExitSubscriber); //função que é invocada quando o método remoto assincrono retorna(OnExit)
                delFuncaoAssincronaSubscriber del = new delFuncaoAssincronaSubscriber(obj.recebeComando); //funcao que vai ser invocada assincronamente(recebeComando)
                escreveLog(separador[2], sub.ID, evento.getTopic()); //[2] = Subscribe/Unsubscribe

                ar = del.BeginInvoke(separador[2], evento, false, funcaoCallBack, null); //começamos a chamada à função assincrona , [2] se é subscribe ou unsubscribe [3] -> nome do topico                       

            }
            else if (separador[0].Equals("Publisher")) //se for um comando para Publisher
            {
                Publicador pub = listaPublishers.Find(x => x.ID == separador[1]); //para enviar a mensagem para o publisher especificado no comando introduzido
                                                                                  //[1] id do pubs
                Evento evento = new Evento(separador[5]); //[5] topico do evento a publicar 
                IPublisher obj = (IPublisher)Activator.GetObject(typeof(IPublisher), pub.Url);
                funcaoCallBack = new AsyncCallback(OnExitPublisher); //função que é invocada quando o método remoto assincrono retorna(OnExit)
                delFuncaoAssincronaPublisher del = new delFuncaoAssincronaPublisher(obj.recebeComando); //funcao que vai ser invocada assincronamente(recebeComando)
                escreveLogPublisher(separador[2], pub.ID, evento.getTopic(), separador[3], separador[7]); //[2] = Publish, [3]= numeroPublicacoes [7] = intervalo entre publicacoes

                ar = del.BeginInvoke(Int32.Parse(separador[3]), evento, Int32.Parse(separador[7]), false, funcaoCallBack, null); //começamos a chamada à função assincrona; [5] nome do topico                   

            }
            else if (separador[0].Equals("Status"))
            {
                foreach (Broker bro in listaBrokers)
                {
                    try {
                        IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), bro.Url);
                        funcaoCallBack = new AsyncCallback(OnExit2); //função que é invocada quando o método remoto assincrono retorna(OnExit)
                        delStatus del = new delStatus(broker.recebeStatus);
                        ar = del.BeginInvoke(funcaoCallBack, null);
                    }catch(Exception e) { }
                }

                foreach (Publicador pub in listaPublishers)
                {
                    IPublisher publisher = (IPublisher)Activator.GetObject(typeof(IPublisher), pub.Url);
                    funcaoCallBack = new AsyncCallback(OnExit2); //função que é invocada quando o método remoto assincrono retorna(OnExit)
                    delStatus del = new delStatus(publisher.recebeStatus);
                    ar = del.BeginInvoke(funcaoCallBack, null);
                }

                foreach (Subscritor sub in listaSubscribers)
                {
                    ISubscriber subscriber = (ISubscriber)Activator.GetObject(typeof(ISubscriber), sub.Url);
                    funcaoCallBack = new AsyncCallback(OnExit2); //função que é invocada quando o método remoto assincrono retorna(OnExit)
                    delStatus del = new delStatus(subscriber.recebeStatus);
                    ar = del.BeginInvoke(funcaoCallBack, null);
                }
            }
            else if (separador[0].Equals("Freeze") || separador[0].Equals("Unfreeze"))
            {
                Broker bro = listaBrokers.Find(x => x.ID.Equals(separador[1]));
                if (bro != null)
                {
                    IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), bro.Url);
                    funcaoCallBack = new AsyncCallback(OnExitPublisher); //função que é invocada quando o método remoto assincrono retorna(OnExit)
                    delBroker del = new delBroker(broker.recebeEvento);

                    escreveLog(separador[0], bro.ID, ""); //[0] = Freeze/Unfreeze

                    //em relação ao subscriber é necessário parar a receção de comandos e de eventos
                    if (separador[0].Equals("Freeze")) //true, fazemos freeze                                           
                        ar = del.BeginInvoke(null, true, "", funcaoCallBack, null);
                    else if(separador[0].Equals("Unfreeze"))//se for unfreeze enviamos false para desbloquear quem estao à espera                   
                        ar = del.BeginInvoke(null, false, "", funcaoCallBack, null);
                    
                }

                Subscritor sub = listaSubscribers.Find(x => x.ID.Equals(separador[1]));
                if (sub != null)
                {
                    ISubscriber subscriber = (ISubscriber)Activator.GetObject(typeof(ISubscriber), sub.Url);
                    funcaoCallBack = new AsyncCallback(OnExitSubscriber); //função que é invocada quando o método remoto assincrono retorna(OnExit)
                    delFuncaoAssincronaSubscriber del = new delFuncaoAssincronaSubscriber(subscriber.recebeComando);
                    delSubscriberEvento del1 = new delSubscriberEvento(subscriber.recebeEvento);

                    escreveLog(separador[0], sub.ID, ""); //[0] = Freeze/Unfreeze

                    //em relação ao subscriber é necessário parar a receção de comandos e de eventos
                    if (separador[0].Equals("Freeze")) //true, fazemos freeze
                    {
                        ar = del.BeginInvoke("", null, true, funcaoCallBack, null);
                        ar = del1.BeginInvoke(null, true, funcaoCallBack, null);
                    }
                    else //se for unfreeze enviamos false para desbloquear quem estao à espera
                    {
                        ar = del.BeginInvoke("", null, false, funcaoCallBack, null);
                        ar = del1.BeginInvoke(null, false, funcaoCallBack, null);
                    }
                }

                Publicador pub = listaPublishers.Find(x => x.ID.Equals(separador[1]));
                if (pub != null)
                {
                    IPublisher publisher = (IPublisher)Activator.GetObject(typeof(IPublisher), pub.Url);
                    funcaoCallBack = new AsyncCallback(OnExitPublisher); //função que é invocada quando o método remoto assincrono retorna(OnExit)
                    delFuncaoAssincronaPublisher del = new delFuncaoAssincronaPublisher(publisher.recebeComando);

                    escreveLog(separador[0], pub.ID, ""); //[0] = Freeze/Unfreeze

                    if (separador[0].Equals("Freeze")) //true, fazemos freeze
                        ar = del.BeginInvoke(0, null, 0, true, funcaoCallBack, null);
                    else //se for unfreeze enviamos false para desbloquear quem estao à espera
                        ar = del.BeginInvoke(0, null, 0, false, funcaoCallBack, null);
                }
            }
            else if (separador[0].Equals("Wait"))
            {
                escreveLog(separador[0], "", separador[1]); //[0] Wait //[1] = Tempo do wait
                Thread.Sleep(Int32.Parse(separador[1])); //separador[1] -> tempo que o PM faz Wait
            }
            else if(separador[0].Equals("Crash"))
            {
                Broker bro = listaBrokers.Find(x => x.ID.Equals(separador[1]));

                IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), bro.Url);
                funcaoCallBack = new AsyncCallback(OnExit2); //função que é invocada quando o método remoto assincrono retorna(OnExit)
                delStatus del = new delStatus(broker.recebeCrash);
                ar = del.BeginInvoke(funcaoCallBack, null);

                escreveLog(separador[0], bro.ID, ""); //[0] = Crash

            }
        }

        public static void escreveLog(string tipoLog, string id, string evento)
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 18);
            
            lock(lock_)
            {
                if (tipoLog.Equals("Subscribe") || tipoLog.Equals("Unsubscribe"))
                {
                    using (StreamWriter w = new StreamWriter(path + "\\ficheiroLog.txt", append: true))
                    {
                        w.WriteLine(DateTime.Now + " : Subscriber " + id + " " + tipoLog + " " + evento);
                    }
                }
                else if (tipoLog.Equals("Freeze") || tipoLog.Equals("Unfreeze"))
                {
                    using (StreamWriter w = new StreamWriter(path + "\\ficheiroLog.txt", append: true))
                    {
                        w.WriteLine(DateTime.Now + " : " + tipoLog + " " + id);
                    }
                }
                else if (tipoLog.Equals("Wait"))
                {
                    using (StreamWriter w = new StreamWriter(path + "\\ficheiroLog.txt", append: true))
                    {
                        w.WriteLine(DateTime.Now + " : " + tipoLog + " " + evento);
                    }
                }
                else if (tipoLog.Equals("Crash"))
                {
                    using (StreamWriter w = new StreamWriter(path + "\\ficheiroLog.txt", append: true))
                    {
                        w.WriteLine(DateTime.Now + " : " + tipoLog + " " + evento);
                    }
                }
            }
        }

        public static void escreveLogPublisher(string tipoLog, string id, string evento, string numeroPublicacoes, string intervalo)
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 18);

            lock (lock_)
            {
                using (StreamWriter w = new StreamWriter(path + "\\ficheiroLog.txt", append: true))
                {
                    w.WriteLine(DateTime.Now + " : Publisher " + id + " " + tipoLog + " " + numeroPublicacoes + " Ontopic " + evento + " Interval " + intervalo);
                }
            }
        }

        public static void OnExitSubscriber(IAsyncResult ar)
        {
            delFuncaoAssincronaSubscriber del = (delFuncaoAssincronaSubscriber)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }
        public static void OnExitPublisher(IAsyncResult ar)
        {
            delFuncaoAssincronaPublisher del = (delFuncaoAssincronaPublisher)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }
        public static void OnExitBroker(IAsyncResult ar)
        {
            delEnviaIrmaos del = (delEnviaIrmaos)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        public static void OnExit2(IAsyncResult ar)
        {
            delStatus del = (delStatus)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }
    }


    public class puppetObject : MarshalByRefObject, IPuppet
    {
        private string loggingLevel = PuppetMaster.loggingLevel;

        public void recebePubLog(string id, string topic, string eventNumber)
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 18);

            lock (PuppetMaster.lock_)
            {
                using (StreamWriter w = new StreamWriter(path + "\\ficheiroLog.txt", append: true))
                {
                    w.WriteLine(DateTime.Now + " : " + "PubEvent " + id + ", " + id + ", " + topic + ", " + eventNumber);
                }
            }
        }

        public void recebeBroLog(string idBroker, string idPublisher, string topic, string eventNumber, string idSubscriber)
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 18);

            lock(PuppetMaster.lock_)
            {
                if(loggingLevel.Equals("full"))
                    using (StreamWriter w = new StreamWriter(path + "\\ficheiroLog.txt", append: true))
                    {
                        w.WriteLine(DateTime.Now + " : " + "BroEvent " + idBroker + ", " + idPublisher + ", " + topic + ", " + eventNumber + ", " + idSubscriber);
                    }
            }
        }

        public void recebeSubLog(string idSubscriber, string idPublisher, string topic, string eventNumber)
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 18);

            lock (PuppetMaster.lock_)
            {
                using (StreamWriter w = new StreamWriter(path + "\\ficheiroLog.txt", append: true))
                {
                    w.WriteLine(DateTime.Now + " : " + "SubEvent " + idSubscriber + ", " + idPublisher + ", " + topic + ", " + eventNumber);
                }
            }
        }
    }

}