// 周期性任务管理主程序
import { TaskScheduler, CONFIG } from './task-scheduler.mjs';
import { readFileSync, statSync, readdirSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join, extname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// 加载任务
// ⚠️ 默认从项目根目录下的 task 文件夹读取任务列表
// 如果路径指向文件夹，会读取文件夹中所有 .json 文件并合并
// 如果路径指向文件，则直接读取该文件
const TASKS_PATH = process.env.TASKS_FILE || join(__dirname, '..', 'task');

function loadTasks() {
  try {
    const stats = statSync(TASKS_PATH);
    let allTasks = [];
    let loadedFiles = [];
    
    if (stats.isDirectory()) {
      // 如果是文件夹，读取文件夹中所有 .json 文件
      console.log(`[CONFIG] Loading tasks from directory: ${TASKS_PATH}`);
      const files = readdirSync(TASKS_PATH).filter(f => extname(f).toLowerCase() === '.json');
      
      if (files.length === 0) {
        throw new Error(`No JSON files found in directory: ${TASKS_PATH}`);
      }
      
      for (const file of files) {
        const filePath = join(TASKS_PATH, file);
        try {
          const fileContent = readFileSync(filePath, 'utf8');
          const data = JSON.parse(fileContent);
          const tasks = Array.isArray(data) ? data : (data.tasks || []);
          
          if (Array.isArray(tasks) && tasks.length > 0) {
            allTasks = allTasks.concat(tasks);
            loadedFiles.push(`${file} (${tasks.length} tasks)`);
            console.log(`[CONFIG]   ✓ Loaded ${tasks.length} task(s) from ${file}`);
          }
        } catch (err) {
          console.warn(`[CONFIG]   ⚠️ Failed to load ${file}: ${err.message}`);
        }
      }
      
      if (allTasks.length === 0) {
        throw new Error(`No valid tasks found in any JSON file in directory: ${TASKS_PATH}`);
      }
      
      console.log(`[CONFIG] Loaded ${allTasks.length} task(s) from ${loadedFiles.length} file(s)`);
    } else if (stats.isFile()) {
      // 如果是文件，直接读取
      console.log(`[CONFIG] Loading tasks from file: ${TASKS_PATH}`);
      const fileContent = readFileSync(TASKS_PATH, 'utf8');
      const data = JSON.parse(fileContent);
      allTasks = Array.isArray(data) ? data : (data.tasks || []);
      
      if (!Array.isArray(allTasks) || allTasks.length === 0) {
        throw new Error('Tasks file must contain a non-empty array of tasks');
      }
      
      console.log(`[CONFIG] Loaded ${allTasks.length} task(s) from ${TASKS_PATH}`);
    } else {
      throw new Error(`Path is neither a file nor a directory: ${TASKS_PATH}`);
    }
    
    // 验证所有任务
    for (let i = 0; i < allTasks.length; i++) {
      const task = allTasks[i];
      if (!task.id) {
        throw new Error(`Task at index ${i} is missing required field 'id'`);
      }
      if (!task.addr) {
        throw new Error(`Task at index ${i} (id: ${task.id}) is missing required field 'addr'`);
      }
    }
    
    return allTasks;
  } catch (error) {
    if (error.code === 'ENOENT') {
      console.error(`[ERROR] Tasks path not found: ${TASKS_PATH}`);
      console.error(`[ERROR] Please create a task directory or file, or set TASKS_FILE environment variable`);
    } else {
      console.error(`[ERROR] Failed to load tasks from ${TASKS_PATH}:`, error.message);
    }
    process.exit(1);
  }
}

async function main() {
  // ⚠️ 设置环境变量，禁用 runbatch.mjs 中的统计输出（调度器有自己的状态报告）
  process.env.SCHEDULED_MODE = 'true';
  process.env.RUN_SCHEDULED = 'true';
  
  console.log('\n' + '='.repeat(70));
  console.log('[SCHEDULER-MAIN] 🎯 Starting Scheduled Task Manager');
  console.log('='.repeat(70));
  console.log(`[SCHEDULER-MAIN][CONFIG] 📋 Configuration:`);
  const headlessMode = process.env.HEADLESS !== 'false';
  console.log(`  🖥️  BROWSER_MODE: ${headlessMode ? 'headless (hidden)' : 'headed (visible)'}`);
  console.log(`  ⛏️  MAX_ACTIVE_MINING: ${CONFIG.MAX_ACTIVE_MINING}`);
  console.log(`  📄 MAX_OPEN_PAGES: ${CONFIG.MAX_OPEN_PAGES}`);
  console.log(`  🔄 PAGE_OPEN_CONCURRENCY: ${CONFIG.PAGE_OPEN_CONCURRENCY}`);
  console.log(`  ⏱️  STATUS_CHECK_INTERVAL: ${CONFIG.STATUS_CHECK_INTERVAL}ms`);
  console.log(`  ⏰ COMPLETION_WAIT_TIME: ${CONFIG.COMPLETION_WAIT_TIME}ms (30s)`);
  console.log('='.repeat(70) + '\n');
  
  // 加载任务
  const tasks = loadTasks();
  console.log(`[SCHEDULER-MAIN] Will manage ${tasks.length} task(s)`);
  
  // 创建调度器
  const scheduler = new TaskScheduler();
  
  // 添加所有任务
  for (const task of tasks) {
    scheduler.addTask(task.id, { addr: task.addr });
  }
  
  // 启动调度器
  console.log('[SCHEDULER-MAIN] 🚀 Starting task scheduler...\n');
  await scheduler.start();
  
  // 定期输出状态
  const statusInterval = setInterval(() => {
    const status = scheduler.getStatus();
    const now = new Date();
    const currentHour = now.getHours();
    
    // 统计各状态的任务数
    const statusCounts = {};
    for (const taskInfo of Object.values(status.tasks)) {
      const s = taskInfo.status;
      statusCounts[s] = (statusCounts[s] || 0) + 1;
    }
    
    const miningUsage = ((status.activeMining / status.maxActiveMining) * 100).toFixed(1);
    const pagesUsage = ((status.openPages / status.maxOpenPages) * 100).toFixed(1);
    
    console.log('\n' + '='.repeat(70));
    console.log(`[SCHEDULER-MAIN][STATUS] 📊 Scheduler Status Report (${now.toISOString()})`);
    console.log(`[SCHEDULER-MAIN][STATUS] Current Hour: ${currentHour}:00 (Cycle ${status.cycle})`);
    console.log('='.repeat(70));
    console.log(`[SCHEDULER-MAIN][STATUS] 🎛️  Control:`);
    console.log(`  Running: ${status.isRunning ? '✓ Yes' : '✗ No'}`);
    console.log(`  Total Tasks: ${status.totalTasks}`);
    console.log(`  Cycle: ${status.cycle}`);
    console.log(`[SCHEDULER-MAIN][STATUS] 💻 Resources:`);
    console.log(`  Active Mining: ${status.activeMining}/${status.maxActiveMining} (${miningUsage}%)`);
    console.log(`  Open Pages: ${status.openPages}/${status.maxOpenPages} (${pagesUsage}%)`);
    console.log(`[SCHEDULER-MAIN][STATUS] 📊 Detailed Statistics:`);
    console.log(`  ✓ Success (Completed): ${status.success}`);
    console.log(`  ✗ Failed: ${status.failed}`);
    console.log(`  🔐 Logging In (before start session page): ${status.loggingIn}`);
    console.log(`  📝 Logged In (at start session page): ${status.loggedIn}`);
    console.log(`  ⛏️ Active Mining (currently mining): ${status.currentlyMining} (Total Started: ${status.miningStarted})`);
    console.log(`  📊 Avg Login Time: ${status.avgLoginTime}s (from ${status.loginTimesCount} tasks)`);
    console.log(`  📊 Avg Mining Time: ${status.avgMiningTime}s (from ${status.miningTimesCount} tasks)`);
    console.log(`[SCHEDULER-MAIN][STATUS] 📈 Task Status Breakdown:`);
    for (const [stat, count] of Object.entries(statusCounts).sort()) {
      const emoji = {
        'pending': '⏸️',
        'initializing': '🔄',
        'mining': '⛏️',
        'completed': '✅',
        'waiting_close': '⏳',
        'closed': '💤',
        'error': '❌',
      }[stat] || '❓';
      console.log(`  ${emoji} ${stat.toUpperCase()}: ${count}`);
    }
    // 计算到下一个整点的时间
    const nextHour = new Date(now);
    nextHour.setHours(currentHour + 1, 0, 0, 0);
    const minutesToNextHour = Math.floor((nextHour - now) / 60000);
    console.log(`[SCHEDULER-MAIN][STATUS] ⏰ Next Cycle Reset: ${currentHour + 1}:00 (${minutesToNextHour} minutes)`);
    console.log('='.repeat(70) + '\n');
  }, 60000); // 每分钟输出一次状态
  
  // 处理退出信号
  const shutdown = async () => {
    console.log('\n[SCHEDULER-MAIN] Shutting down scheduler...');
    clearInterval(statusInterval);
    await scheduler.stop();
    process.exit(0);
  };
  
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  
  // 保持运行
  console.log('[SCHEDULER-MAIN] Scheduler is running. Press Ctrl+C to stop.');
  
  // 无限等待
  await new Promise(() => {});
}

// 运行主函数
main().catch(e => {
  console.error('[SCHEDULER-MAIN] Fatal error:', e);
  process.exit(1);
});
