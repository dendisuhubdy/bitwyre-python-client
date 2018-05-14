from enum import Enum
import logging
import random
from connection import ConnectionState, MessageDirection
from client_connection import FIXClient
from engine import FIXEngine
from message import FIXMessage
from event import TimerEventRegistration

class Side(Enum):
    buy = 1
    sell = 2

class Client(FIXEngine):
    def __init__(self):
        FIXEngine.__init__(self, "client_example.store")
        self.clOrdID = 0
        self.msgGenerator = None

        # create a FIX Client using the FIX 4.4 standard
        self.client = FIXClient(self, "pyfix.FIX44", "BITWYRE_EXCHANGE", "SENDER")

        # we register some listeners since we want to know when the connection goes up or down
        self.client.addConnectionListener(self.onConnect, ConnectionState.CONNECTED)
        self.client.addConnectionListener(self.onDisconnect, ConnectionState.DISCONNECTED)

        # start our event listener indefinitely
        self.client.start('localhost', int("5001"))
        while True:
            self.eventManager.waitForEventWithTimeout(10.0)

        # some clean up before we shut down
        self.client.removeConnectionListener(self.onConnect, ConnectionState.CONNECTED)
        self.client.removeConnectionListener(self.onConnect, ConnectionState.DISCONNECTED)

    def onConnect(self, session):
        logging.info("Established connection to %s" % (session.address(), ))
        # register to receive message notifications on the session which has just been created
        session.addMessageHandler(self.onLogin, MessageDirection.INBOUND, self.client.protocol.msgtype.LOGON)
        session.addMessageHandler(self.onExecutionReport, MessageDirection.INBOUND, self.client.protocol.msgtype.EXECUTIONREPORT)

    def onDisconnect(self, session):
        logging.info("%s has disconnected" % (session.address(), ))
        # we need to clean up our handlers, since this session is disconnected now
        session.removeMessageHandler(self.onLogin, MessageDirection.INBOUND, self.client.protocol.msgtype.LOGON)
        session.removeMessageHandler(self.onExecutionReport, MessageDirection.INBOUND, self.client.protocol.msgtype.EXECUTIONREPORT)
        if self.msgGenerator:
            self.eventManager.unregisterHandler(self.msgGenerator)

    def sendOrder(self, connectionHandler, symbol, securityID, side, quantity, price,
                    SecurityIDSource="4", account="TEST", handlInst="1", ExDestination="XLON", currency="USD"):
        self.clOrdID = self.clOrdID + 1
        codec = connectionHandler.codec
        msg = FIXMessage(codec.protocol.msgtype.NEWORDERSINGLE)
        msg.setField(codec.protocol.fixtags.Price, float(price)) #"%0.2f" % (random.random() * 2 + 10))
        msg.setField(codec.protocol.fixtags.OrderQty, int(quantity)) #int(random.random() * 100))
        msg.setField(codec.protocol.fixtags.Symbol, str(symbol))
        msg.setField(codec.protocol.fixtags.SecurityID, str(securityID))
        msg.setField(codec.protocol.fixtags.SecurityIDSource, str(securityIDSource))
        msg.setField(codec.protocol.fixtags.Account, str(account))
        msg.setField(codec.protocol.fixtags.HandlInst, str(handlInst))
        msg.setField(codec.protocol.fixtags.ExDestination, str(ExDestination))
        msg.setField(codec.protocol.fixtags.Side, int(side)) #int(random.random() * 2) + 1)
        msg.setField(codec.protocol.fixtags.ClOrdID, str(self.clOrdID))
        msg.setField(codec.protocol.fixtags.Currency, str(currency))

        connectionHandler.sendMsg(msg)
        side = Side(int(msg.getField(codec.protocol.fixtags.Side)))
        logging.debug("---> [%s] %s: %s %s %s@%s" % (codec.protocol.msgtype.msgTypeToName(msg.msgType), 
                                                    msg.getField(codec.protocol.fixtags.ClOrdID), 
                                                    msg.getField(codec.protocol.fixtags.Symbol), 
                                                    side.name, 
                                                    msg.getField(codec.protocol.fixtags.OrderQty), 
                                                    msg.getField(codec.protocol.fixtags.Price)))

    def userInput(self):
        symbol      = input('Enter the execution Symbol:')
        symbolID    = input('Enter the execution SymbolID:')
        side        = input('Enter the execution Buy/Sell:')
        price       = input('Enter the execution Price:')
        quantity    = input('Enter the execution Quantity:')
        return symbol, symbolID, side, price, quantity

    def onLogin(self, connectionHandler, msg):
        logging.info("Logged in")

        # lets do something like send an order every 3 seconds
        symbol, symbolID, side, price, quantity = userInput()
        self.msgGenerator = EventRegistration(lambda type, closure: self.sendOrder(closure, symbol, symbolID, side, price, quantity), connectionHandler)
        self.eventManager.registerHandler(self.msgGenerator)

    def onExecutionReport(self, connectionHandler, msg):
        codec = connectionHandler.codec
        if codec.protocol.fixtags.ExecType in msg:
            if msg.getField(codec.protocol.fixtags.ExecType) == "0":
                side = Side(int(msg.getField(codec.protocol.fixtags.Side)))
                logging.debug("<--- [%s] %s: %s %s %s@%s" % (codec.protocol.msgtype.msgTypeToName(msg.getField(codec.protocol.fixtags.MsgType)), msg.getField(codec.protocol.fixtags.ClOrdID), msg.getField(codec.protocol.fixtags.Symbol), side.name, msg.getField(codec.protocol.fixtags.OrderQty), msg.getField(codec.protocol.fixtags.Price)))
            elif msg.getField(codec.protocol.fixtags.ExecType) == "4":
                reason = "Unknown" if codec.protocol.fixtags.Text not in msg else msg.getField(codec.protocol.fixtags.Text)
                logging.info("Order Rejected '%s'" % (reason,))
        else:
            logging.error("Received execution report without ExecType")

def main():
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
    client      = Client()
    logging.info("All done... shutting down")

if __name__ == '__main__':
    main()
