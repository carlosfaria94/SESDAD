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

namespace PuppetMasterURL
{
    class PuppetMasterURL
    {
        public static List<Broker> listaBrokers = new List<Broker>();
        public static List<Publicador> listaPublishers = new List<Publicador>();
        public static List<Subscritor> listaSubscribers = new List<Subscritor>();

        public delegate void delFuncaoAssincronaSubscriber(string comando, Evento evento, bool freeze);
        public delegate void delFuncaoAssincronaPublisher(int numeroPublicacoes, Evento evento, int intervaloEntrePublicacoes, bool freeze);
        public delegate void delStatus();
        public delegate void delBroker(Evento evento, bool freeze, string urlFilho);
        public delegate void delSubscriberEvento(Evento evento, bool freeze);

        static AsyncCallback funcaoCallBack; //irá chamar uma função quando a função assincrona terminar
        static IAsyncResult ar;

        static void Main(string[] args)
        {
            TcpChannel channel = new TcpChannel(10000);
            ChannelServices.RegisterChannel(channel, false);
            RemotingConfiguration.RegisterWellKnownServiceType(typeof(puppetObjectRemoto), "PuppetMasterObjectURL", WellKnownObjectMode.Singleton);

            while (true)
            {
                string comando = Console.ReadLine();
                executaComando(comando);
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
                Broker bro = listaBrokers.Find(x => x.ID == separador[1]); //para enviar a mensagem para o publisher especificado no comando introduzido
                if (bro != null)
                {
                    IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), bro.Url);
                    funcaoCallBack = new AsyncCallback(OnExitPublisher); //função que é invocada quando o método remoto assincrono retorna(OnExit)
                    delStatus del = new delStatus(broker.recebeStatus);
                }

                Publicador pub = listaPublishers.Find(x => x.ID == separador[1]); //para enviar a mensagem para o publisher especificado no comando introduzido
                if (pub != null)
                {
                    IPublisher publisher = (IPublisher)Activator.GetObject(typeof(IPublisher), pub.Url);
                    funcaoCallBack = new AsyncCallback(OnExitPublisher); //função que é invocada quando o método remoto assincrono retorna(OnExit)
                    delStatus del = new delStatus(publisher.recebeStatus);
                    ar = del.BeginInvoke(funcaoCallBack, null);
                }

                Subscritor sub = listaSubscribers.Find(x => x.ID == separador[1]); //para enviar a mensagem para o subscriber especificado no comando introduzido
                if (sub != null)
                {
                    ISubscriber subscriber = (ISubscriber)Activator.GetObject(typeof(ISubscriber), sub.Url);
                    funcaoCallBack = new AsyncCallback(OnExitSubscriber); //função que é invocada quando o método remoto assincrono retorna(OnExit)
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
                    ar = del.BeginInvoke(null, false, "", funcaoCallBack, null);

                    escreveLog(separador[0], bro.ID, ""); //[0] = Freeze/Unfreeze


                    //em relação ao subscriber é necessário parar a receção de comandos e de eventos
                    if (separador[0].Equals("Freeze")) //true, fazemos freeze
                        ar = del.BeginInvoke(null, true, "", funcaoCallBack, null);
                    else //se for unfreeze enviamos false para desbloquear quem estao à espera
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
        }

        public static void escreveLog(string tipoLog, string id, string evento)
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 26);

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
        }

        public static void escreveLogPublisher(string tipoLog, string id, string evento, string numeroPublicacoes, string intervalo)
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 26);

            using (StreamWriter w = new StreamWriter(path + "\\ficheiroLog.txt", append: true))
            {
                w.WriteLine(DateTime.Now + " : Publisher " + id + " " + tipoLog + " " + numeroPublicacoes + " Ontopic " + evento + " Interval " + intervalo);
            }
        }

        public static void OnExitSubscriber(IAsyncResult ar)
        {
            delFuncaoAssincronaSubscriber del = (delFuncaoAssincronaSubscriber)((AsyncResult)ar).AsyncDelegate;
            Console.WriteLine("Funcao Assincrona retornou");
            del.EndInvoke(ar);
        }
        public static void OnExitPublisher(IAsyncResult ar)
        {
            delFuncaoAssincronaPublisher del = (delFuncaoAssincronaPublisher)((AsyncResult)ar).AsyncDelegate;
            Console.WriteLine("Funcao Assincrona retornou");
            del.EndInvoke(ar);
        }

    }

    public class puppetObjectRemoto : MarshalByRefObject, IPuppetMasterURL
    {
        AsyncCallback funcaoCallBack; //irá chamar uma função quando a função assincrona terminar
        IAsyncResult ar;
        public delegate void delPolicies(string policy, bool criaTabela);
        public delegate void delPolicies2(string policy);
        private string loggingLevel = "light"; //default é light

        public void invocaProcessoBroker(string id, string url, string urlBrokerPai, int numeroBrokers, string routingPolicy, string orderingPolicy, string loggingLevel, string urlPuppet, string urlIrmao1, string urlIrmao2, string site, bool criaTabela)
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 26);

            string[] urlAux = url.Split(':'); //separamos para podermos obter o porto para inicializar o canal 
            string[] port = urlAux[2].Split('/'); //temos que voltar a separar para obter finalmente o porto que se encontra em port[0]
            string[] portAux = port[0].Split('/');
            int portFinal = Int32.Parse(portAux[0]);

            Broker bro = new Broker(id, portFinal, url);
            PuppetMasterURL.listaBrokers.Add(bro);

            this.loggingLevel = loggingLevel;

            if (!urlBrokerPai.Equals("raiz"))
                Process.Start(path + @"\\Broker\\bin\\Debug\\Broker.exe", id + " " + url + " " + urlBrokerPai + " " + numeroBrokers + " " + loggingLevel + " " + urlPuppet + " "  + urlIrmao1 + " " + urlIrmao2 + " " + site);
            else
                Process.Start(path + @"\\Broker\\bin\\Debug\\Broker.exe", id + " " + url + " " + "raiz" + " " + numeroBrokers + " " + loggingLevel + " " + urlPuppet + " " + urlIrmao1 + " " + urlIrmao2 + " " + site);
      
           
            //enviamos as policies para o broker
            IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), url);
            funcaoCallBack = new AsyncCallback(OnExitSubscriber); //função que é invocada quando o método remoto assincrono retorna(OnExit)
            delPolicies del = new delPolicies(broker.setRoutingPolicy);
            ar = del.BeginInvoke(routingPolicy, criaTabela, funcaoCallBack, null); 

            funcaoCallBack = new AsyncCallback(OnExitSubscriber); //função que é invocada quando o método remoto assincrono retorna(OnExit)
            delPolicies2 del1 = new delPolicies2(broker.setOrderingPolicy);
            ar = del1.BeginInvoke(orderingPolicy, funcaoCallBack, null); 
        }

        public void invocaProcessoPublisher(string id, string url, string urlBroker1, string urlBroker2, string urlBroker3, string urlPuppet)
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 26);

            string[] urlAux = url.Split(':'); //separamos para podermos obter o porto para inicializar o canal 
            string[] port = urlAux[2].Split('/'); //temos que voltar a separar para obter finalmente o porto que se encontra em port[0]
            string[] portAux = port[0].Split('/');
            int portFinal = Int32.Parse(portAux[0]);

            Publicador pub = new Publicador(id, portFinal, url);
            PuppetMasterURL.listaPublishers.Add(pub);

            Process.Start(path + @"\\Publisher\\bin\\Debug\\Publisher.exe", id + " " + url + " " + urlBroker1 + " " + urlBroker2 + " " + urlBroker3 + " " + urlPuppet);
        }

        public void invocaProcessoSubscriber(string id, string url, string urlBroker1, string urlBroker2, string urlBroker3, string urlPuppet)
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 26);

            string[] urlAux = url.Split(':'); //separamos para podermos obter o porto para inicializar o canal 
            string[] port = urlAux[2].Split('/'); //temos que voltar a separar para obter finalmente o porto que se encontra em port[0]
            string[] portAux = port[0].Split('/');
            int portFinal = Int32.Parse(portAux[0]);

            Subscritor sub = new Subscritor(id, portFinal, url);
            PuppetMasterURL.listaSubscribers.Add(sub);

            Process.Start(path + @"\\Subscriber\\bin\\Debug\\Subscriber.exe", id + " " + url + " " + urlBroker1 + " " + urlBroker2 + " " + urlBroker3 + " " + urlPuppet);
        }

        public void recebePubLog(string id, string topic, string eventNumber)
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 26);

            using (StreamWriter w = new StreamWriter(path + "\\ficheiroLog.txt", append: true))
            {
                w.WriteLine(DateTime.Now + " : " + "PubEvent " + id + ", " + id + ", " + topic + ", " + eventNumber);
            }
        }

        public void recebeBroLog(string idBroker, string idPublisher, string topic, string eventNumber)
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 26);

            if(loggingLevel.Equals("full"))
                using (StreamWriter w = new StreamWriter(path + "\\ficheiroLog.txt", append: true))
                {
                    w.WriteLine(DateTime.Now + " : " + "BroEvent " + idBroker + ", " + idPublisher + ", " + topic + ", " + eventNumber);
                }
        }

        public void recebeSubLog(string idSubscriber, string idPublisher, string topic, string eventNumber)
        {
            string path = Directory.GetCurrentDirectory();
            path = path.Remove(path.Length - 26);

            using (StreamWriter w = new StreamWriter(path + "\\ficheiroLog.txt", append: true))
            {
                w.WriteLine(DateTime.Now + " : " + "SubEvent " + idSubscriber + ", " + idPublisher + ", " + topic + ", " + eventNumber);
            }
        }

        public static void OnExitSubscriber(IAsyncResult ar)
        {
            delPolicies del = (delPolicies)((AsyncResult)ar).AsyncDelegate;
            Console.WriteLine("Funcao Assincrona retornou");
            del.EndInvoke(ar);
        }
    }
}

