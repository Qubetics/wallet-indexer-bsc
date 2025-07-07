import kafkaHelper from "./config/kafka.helper";
import { bscWithdrawDepositProcess, bscPendingWithdrawalProcess, bscTxStatusUpdateProcess, bscBlocksProcess, processedBlockTransactions, updateBalanceProcess } from "./processes/index.process"
import schedule from "node-schedule";
class BSCStartProcess {
  constructor() {
    this.connectKafka();
    this.readBlockProcess();
    this.readBehindBlocks();
    this.readSpecificBlock();
    this.consumeBlockTxQueue();
    this.consumeBehindBlockTxQueue();
    this.consumeWithdrawDepositTxsQueue();
    this.internalTxsInQueue();
    this.consumeInternalTxQueue();
    this.consumeUpdateBalanceQueue();
  }

  public async connectKafka() {
    await kafkaHelper.connect();
  }

  public readBlockProcess() {
    schedule.scheduleJob("*/8 * * * * *", async function () {
      await bscBlocksProcess.readCurrentBlocks();
    });
  }

  public readBehindBlocks() {
    schedule.scheduleJob("*/8 * * * * *", async function () {
      await bscBlocksProcess.readBehindBlock();
    });
  }

  // public testing_node() {
  //   schedule.scheduleJob("*/2 * * * * *", async function () {
  //     await bsc_blocks_process.testingNode();
  //   });
  // }

  public readSpecificBlock() {
    // 1 minute
    schedule.scheduleJob("*/1 * * * *", async function () {
      await bscBlocksProcess.readSpecificBlock();
    });
  }

  public consumeBlockTxQueue() {
    setTimeout(async () => {
      await processedBlockTransactions.startBlockTxQueue();
    }, 10000);
  }

  public consumeBehindBlockTxQueue() {
    setTimeout(async () => {
      await processedBlockTransactions.startBehindBlockTxQueue();
    }, 10000);
  }

  public async consumeWithdrawDepositTxsQueue() {
    setTimeout(async () => {
      await bscWithdrawDepositProcess.startDepositQueue();
    }, 10000);
  }

  public internalTxsInQueue() {
    // This runs every 10 seconds (*/10 * * * * *)
    schedule.scheduleJob("*/10 * * * * *", async function () {
      await bscPendingWithdrawalProcess.getTransactionFromDB();
    });
  }

  public async consumeInternalTxQueue() {
    setTimeout(async () => {
      await bscTxStatusUpdateProcess.startTxStatusUpdateQueue();
    }, 10000);
  }

  public async consumeUpdateBalanceQueue() {
    setTimeout(async () => {
      await updateBalanceProcess.updateWalletBalance();
    }, 10000);
  }
}

new BSCStartProcess();
