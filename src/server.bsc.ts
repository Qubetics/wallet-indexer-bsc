import kafkaHelper from "./config/kafka.helper";
import {
  readBlockProcess,
  depositWithdrawProcess,
  readInternalTxProcess,
  updateInternalTrnxStatusProcess,
  readBlockTrnxProcess,
  updateBalanceProcess,
  traceBlockBscProcess
} from "./processes/index.process";
import schedule from "node-schedule";
class BSCStartProcess {
  constructor() {
    this.connectKafka();
    this.readBlockProcess();
    this.readBehinedBlockProcess();
    this.readSpecificBlock();
    this.startBlocksConsumers();
    this.startDepositWithdrawConsumer();
    this.internalWithdrawalTxProcess();
    this.startInternalTxStatusUpdateConsumer();
    this.startUpdateBalanceConsumer();
    this.updateBalanceByWallet();
    this.consumeTraceBlockBscTxsQueue();
  }
  private async connectKafka() {
    await kafkaHelper.connect();
  }

  
  private readBlockProcess() {
    setInterval(async () => {
      await readBlockProcess.readCurrentBlocks();
    }, 700);
  }

  private readBehinedBlockProcess() {
    schedule.scheduleJob("*/2 * * * * *", async function () {
      await readBlockProcess.readBehindBlock()
    });
  }

  
  private readSpecificBlock() {
    console.log("readSpecificBlock started after 1 min 🔆🔆🔆");
    schedule.scheduleJob('*/15 * * * * *', async function () {


      await readBlockTrnxProcess.readSpecificBlock();
    })
  }

  private async consumeTraceBlockBscTxsQueue() {
    setTimeout(async () => {
      console.log("consumer TraceBlockBscTxsQueue started 🔆🔆🔆");
      await traceBlockBscProcess.startTraceBlockBscProcess();
    }, 1000);
  }

  private startBlocksConsumers() {
    setTimeout(async () => {
      readBlockTrnxProcess.startLatestBlockEvenConsumer();
      readBlockTrnxProcess.startLatestBlockOddConsumer();
      readBlockTrnxProcess.startBehindBlockConsumer();
    }, 1000);
  }


  private async startDepositWithdrawConsumer() {
    setTimeout(async () => {
      await depositWithdrawProcess.startDepositWithdrawConsumer();
    }, 1000);
  }
  private internalWithdrawalTxProcess() {
    schedule.scheduleJob("*/10 * * * * *", async function () {
      await readInternalTxProcess.getTransactionFromDB();
    });
  }
  private async startInternalTxStatusUpdateConsumer() {
    setTimeout(async () => {
      await updateInternalTrnxStatusProcess.startTxStatusUpdateConsumer();
    }, 1000);
  }

  private async startUpdateBalanceConsumer() {
    setTimeout(async () => {
      await updateBalanceProcess.updateBalanceConsumer();
    }, 1000);
  }

  private async updateBalanceByWallet() {
    schedule.scheduleJob("*/2 * * * *", async function () {
      await updateBalanceProcess.updateWalletBalance();
    });
  }
}

new BSCStartProcess();
