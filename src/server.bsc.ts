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
    // this.read_block_process();
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
  // public read_block_process() {
  //   schedule.scheduleJob("* * * * * *", async function () {
  //     await bsc_blocks_process.getBlocks();
  //   });
  // }
  // private readBlockProcess() {
  //   setInterval(async () => {
  //     await bscBlocksProcess.getBlocks();
  //   }, 700);
  // }

  


  // Need to discuess this with team
  // private startBlocksConsumers() {
  //   setTimeout(async () => {
  //     // console.log("consume_read_block_queue >>>>>>");
  //     readBlockTrnxProcess.startLatestBlockEvenConsumer();
  //     readBlockTrnxProcess.startLatestBlockOddConsumer();
  //     readBlockTrnxProcess.startBehindBlockConsumer();
  //     // await bsc_tx_status_deposit_withdraw.consume_queue_trnx();
  //   }, 1000);
  // }
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

      // schedule.scheduleJob('*/1 * * * *', async function () {

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
      // console.log("consume_read_block_queue >>>>>>");
      readBlockTrnxProcess.startLatestBlockEvenConsumer();
      readBlockTrnxProcess.startLatestBlockOddConsumer();
      readBlockTrnxProcess.startBehindBlockConsumer();
      // await bsc_tx_status_deposit_withdraw.consume_queue_trnx();
    }, 1000);
  }

  // public read_behined_block_process() {
  //   schedule.scheduleJob("*/2 * * * * *", async function () {
  //     await bsc_blocks_process.readBehindBlock();
  //   });
  // }
  // public testing_node() {
  //   schedule.scheduleJob("*/2 * * * * *", async function () {
  //     await bsc_blocks_process.testingNode();
  //   });
  // }


  //old code
  // public readSpecificBlock() {
  //   schedule.scheduleJob("*/1 * * * *", async function () {
  //     await readBlockTrnxProcess.readSpecificBlock();
  //   });
  // }

  




  // public startBlockTrxConsumers() {
  //   setTimeout(async () => {
  //     // console.log("consume_read_block_queue >>>>>>");
  //     await readTransactionsProcess.startBlockTxConsumer();
  //     await readTransactionsProcess.startBehindBlockTxConsumer();
  //     // await bsc_tx_status_deposit_withdraw.consume_queue_trnx();
  //   }, 1000);
  // }

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
