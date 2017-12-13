package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.configuration.Configuration;

public class DetectionOnboardServiceExecutor implements DetectionOnboardService {

  private ConcurrentHashMap<Long, DetectionOnboardJobStatus> detectionOnboardJobStatus;
  private ConcurrentHashMap<String, Long> jobNameToId;
  private AtomicLong jobIdCounter = new AtomicLong();
  private ExecutorService executorService;

  public void start() {
    detectionOnboardJobStatus = new ConcurrentHashMap<>();
    jobNameToId = new ConcurrentHashMap<>();
    executorService = Executors.newCachedThreadPool();
  }

  public void shutdown() {
    AnomalyUtils.safelyShutdownExecutionService(executorService, DetectionOnboardServiceExecutor.class);
  }

  @Override
  public long createDetectionOnboardingJob(DetectionOnboardJob job, Map<String, String> properties) {
    Preconditions.checkNotNull(detectionOnboardJobStatus, "Job executor is not initialized");
    Preconditions.checkNotNull(jobNameToId, "Job executor is not initialized");
    Preconditions.checkNotNull(executorService, "Job executor is not initialized");
    Preconditions.checkNotNull(job, "Job cannot be null.");
    Preconditions.checkNotNull(job.getName(), "Job name cannot be null.");
    Preconditions.checkNotNull(properties, String.format("Job %s has null property.", job.getName()));

    final long jobId = jobIdCounter.getAndIncrement();

    String jobName = job.getName();
    // Duplicated job name will not be executed, which could happen in the following two cases:
    // 1. A concurrent thread has insert the job before current thread.
    // 2. A job with the same name is submitted again after a while.
    Long previousJobId = jobNameToId.putIfAbsent(jobName, jobId);
    if (previousJobId != null) {
      throw new IllegalArgumentException(String.format("Cannot create duplicated job: %s", jobName));
    } else {
      // Initialize the job
      // TODO: Decide if we should throw exception or create a job status with job status = failed.
      ImmutableMap<String, String> immutableProperties = ImmutableMap.copyOf(properties);
      job.initialize(immutableProperties);
      Configuration configuration = job.getTaskConfiguration();
      Preconditions.checkNotNull(configuration, String.format("Job %s returns a null configuration.", jobName));

      List<DetectionOnboardTask> tasks = job.getTasks();
      Preconditions.checkNotNull(tasks, "Job %s returns a null task list.", jobName);
      Preconditions.checkArgument(!hasDuplicateTaskName(tasks), "Job %s contains duplicate task name.", jobName);

      DetectionOnboardJobStatus jobStatus = new DetectionOnboardJobStatus();
      jobStatus.setJobId(jobId);
      jobStatus.setJobStatus(JobConstants.JobStatus.SCHEDULED);
      DetectionOnboardJobContext jobContext = new DetectionOnboardJobContext(jobName, jobId, configuration);
      DetectionOnBoardJobRunner jobRunner = new DetectionOnBoardJobRunner(jobContext, tasks, jobStatus);

      // Submit the job to executor
      executorService.submit(jobRunner);
      detectionOnboardJobStatus.put(jobId, jobStatus);
    }

    return jobId;
  }

  private boolean hasDuplicateTaskName(List<DetectionOnboardTask> tasks) {
    Set<String> taskNames = new HashSet<>();
    for (DetectionOnboardTask task : tasks) {
      if (!taskNames.add(task.getTaskName())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public DetectionOnboardJobStatus getDetectionOnboardingJobStatus(long jobId) {
    return detectionOnboardJobStatus.get(jobId);
  }
}
