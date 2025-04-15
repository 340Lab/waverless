\begin{abstract}
Serverless computing has transformed cloud resource management; however, the separation of scaling and scheduling in current methods leads to low resource utilization, increased cold start delays, and high operational costs. This paper presents a joint optimization mechanism, Cejoss, which integrates a reinforcement learning-based scaling strategy (RELA) with a data-aware pre-scheduling mechanism (DECODS). By sharing scaling state and task information in real time, the system achieves coordinated decision-making between scaling and scheduling to achieve joint optimization. With this joint optimization, the system is able to dynamically adjust the number of instances and pre-schedule tasks to reduce cold start delays, ultimately achieving a near-optimal resource allocation. Moreover, the scaler utilizes reinforcement learning to anticipate workload fluctuations and efficiently regulate instance counts, while the scheduler employs a data-aware pre-scheduling strategy to optimize task assignments and minimize latency. Experimental results show that in single-function scenarios the quality-price ratio improves by at least 50\%, while in multi-function scenarios it increases by at least 17\%, demonstrating significant advantages in enhancing system performance and reducing costs. This approach is adaptable to various workload scales and application types, offering a novel perspective for efficient serverless platform operations.
\end{abstract}

\begin{IEEEkeywords}
Cloud Computing, Servreless, Scaler, Scheduler, Joint Optimization
\end{IEEEkeywords}

\section{Introduction}

Serverless computing has become a paradigm shift in cloud resource management, offering auto-scaling, pay-as-you-go pricing, and rapid application development. This innovative approach offers versatile solutions for various workloads, including web services, data analysis, scientific computing, and machine learning inference. Examining the architecture of serverless systems, it can be divided into components like scaler, scheduler, observation and storage. Among these, the scaler and scheduler play critical roles in the performance and cost efficiency of serverless applications. The scaler determines the number and placement of function instances, directly affecting resource utilization and cold start , while the scheduler's decisions on task assignment impact execution latency and load distribution. Their interaction fundamentally shapes the system's ability to handle varying workloads efficiently. 

Current serverless systems face three critical challenges that significantly impact their performance and efficiency. At the architectural level, the lack of coordination between scaling and scheduling components leads to fundamental limitations. This independent operation creates information isolation, where scaling decisions are made without knowledge of scheduling states and vice versa, resulting in suboptimal resource allocation and increased cold start delays.

In the scaling component, current mechanisms rely heavily on static parameters and manual tuning, making them unable to adapt to dynamic workloads. The delayed response to workload changes not only leads to resource inefficiency but also hinders the scheduler's ability to make informed placement decisions. This limitation becomes particularly evident in scenarios with varying request patterns, where static scaling rules fail to provide appropriate resource levels for optimal scheduling.

On the scheduling side, existing strategies lack the sophistication needed for efficient task placement in a modern serverless environment. Traditional approaches either trigger tasks reactively after predecessor completion (causing unnecessary cold starts) or pre-schedule all tasks aggressively (wasting resources). Moreover, they often ignore critical factors like DAG data transmission delays, which significantly impact overall performance. These scheduling limitations, combined with the lack of coordination with scaling decisions, further compound the system's inefficiencies.

To address these challenges, this paper presents Cejoss (Cost-effective Joint Optimization for Scaler and Scheduler), which makes the following key contributions:

(1) The core of our contribution is a novel joint optimization framework \textbf{Cejoss} that merges scaling node selection and scheduling node selection stages. This integration enables real-time sharing of resource views between components, allowing both the scaler and scheduler to maintain timely awareness of each other's decisions. Through this coordinated approach, the system achieves significant improvements in both latency and cost metrics.

(2) To improve dynamic resource management, we introduce \textbf{RELA} (REinforce Learning-based scAler), which employs PPO-based reinforcement learning with carefully designed state space, reward function, and action mapping. RELA achieves adaptive optimization between different application types and request frequencies, demonstrating superior performance in both single-function and multi-function scenarios and effectively addressing the limitations of static scaling parameters.

(3) For efficient task scheduling, we develop \textbf{DECODS} (DEcoupled CO-scaling Data-aware Scheduler), which implements a three-stage scheduling approach comprising Task Collection, Scaling Nodes Selection, and Task Nodes Selection. DECODS introduces moderate pre-scheduling to reduce cold start time while avoiding over-allocation of resources. By considering DAG data transmission latency for optimized task placement, it achieves a balanced trade-off between pre-scheduling benefits and resource efficiency.

(4) Our comprehensive experimental evaluation demonstrates that this integrated approach significantly improves system performance. The quality-price ratio improves by at least 50\% in single-function scenarios and 17\% in multi-function scenarios compared to state-of-the-art approaches. These results validate the effectiveness of our joint optimization strategy across various workload scales and application types.


\section{Background and Motivation}

\subsection{Notations and Terms}

Before discussing the challenges in current serverless systems, we first define several key terms used throughout this paper:

\textbf{Function:} A single, stateless piece of application logic deployed to the cloud. Each function has its own code and resource configuration (memory, timeout, etc.) and is invoked on-demand in response to events or requests. For example, a function might resize an image or process a database query.

\textbf{Instance:} The runtime environment (typically a container or virtual machine) that executes function tasks. An instance has loaded the function code and can serve multiple tasks concurrently. New instances may be started (incurring cold start delays) or terminated by the platform based on demand.

\textbf{Task (Invocation):} A runtime instance of a function execution, triggered by a specific event or request. Each task runs the function code with given input and produces output. Tasks are the units of work that must be scheduled for execution.

\textbf{Quality-Price Ratio:} A comprehensive metric that evaluates both system performance and resource efficiency, calculated as $QP = \frac{Performance}{Cost}$. Performance considers factors like request latency and throughput, while Cost accounts for resource consumption and instance hours.

\textbf{Single-Function Applications:} These applications involve independent function invocations without dependencies. Common examples include API endpoints (e.g., HTTP request handlers), event processors (e.g., image resizing, video transcoding), and stateless microservices. While simpler to manage, they still require efficient scaling to handle varying request rates and optimal instance placement for load balancing.

\textbf{Multi-Function Applications:} These applications compose multiple functions into complex DAGs, where the output of one function serves as input to others. Different applications exhibit varying characteristics in terms of computation and data transfer requirements. Video processing pipelines, for example, involve significant data movement as they transform raw uploads through decode, filter, encode, and thumbnail generation stages. Machine learning applications, on the other hand, often emphasize computational intensity, chaining functions from data preprocessing and feature extraction to model inference and result ranking. Data analytics pipelines combine both aspects, processing logs through collection, parsing, aggregation, and visualization stages, with varying demands on computation and data transfer at different stages. Each stage in these workflows represents a separate function, with both data and control dependencies flowing between them.


\subsection{Scaler and Scheduler}

The core of serverless resource management consists of two key components:

\textbf{Scaler:} The component responsible for dynamic resource management in serverless systems. It continuously monitors system metrics (e.g., request rates, CPU utilization) to determine both the appropriate number of function instances and their optimal placement across nodes. When workload increases, the scaler must decide not only how many new instances to launch, but also which nodes should host them, considering factors like node capacity, current load, and network conditions. Conversely, during periods of low demand, it identifies underutilized instances for termination while maintaining sufficient capacity for incoming requests. These scaling decisions directly impact both system performance (through cold start latency and execution efficiency) and cost (through resource utilization and instance hours).

\textbf{Scheduler:} The component that assigns incoming tasks to available instances. When a task arrives, the scheduler must decide which instance should execute it, considering factors like instance availability, load balancing, and data locality. In modern serverless systems, schedulers must also handle complex application represented as DAGs (Directed Acyclic Graphs), where multiple functions have dependencies and data transfer requirements.

The effectiveness of these components heavily depends on how they interact and coordinate their decisions. In practice, current serverless platforms have explored two fundamentally different architectural patterns. The \textbf{Global Scheduler Only} pattern relies solely on a global scheduler, without an explicit scaling component. When a task arrives, the scheduler assigns it to a node, triggering instance creation if necessary. The instance lifecycle is managed through node-local mechanisms: instances are automatically started when tasks are scheduled (incurring cold starts), and terminated based on local policies such as idle timeouts or cache eviction strategies. This approach simplifies the architecture but leaves scaling decisions to emerge from the combined effects of task placement and local instance management. \textbf{The Decoupled Scaler & Scheduler} pattern adopts a different approach, where separate scaling and scheduling components work independently. The scaler proactively manages instance counts based on global metrics, while the scheduler focuses on task placement among available instances.

Figure \ref{fig:arch_patterns} illustrates these contrasting architectures and their decision flows.

\begin{figure}[htbp]
\centerline{\includegraphics[width=1.0\linewidth]{ArchPatterns.png}}
\caption{Two predominant architectural patterns in serverless platforms: (a) Global Scheduler Only pattern, where task placement triggers instance creation and local policies handle termination; (b) Decoupled pattern, where explicit scaling decisions are made independently of task scheduling.}
\label{fig:arch_patterns}
\end{figure}

\begin{figure}[htbp]
\centerline{\includegraphics[width=1.0\linewidth]{SimpleCompare.png}}
\caption{The metrics of three basic strategies in terms of average request latency, average request cost, and the number of instances tested under medium workload (The latency is composed of a dark part and a light part, with the dark part representing execution latency and the light part representing cold start latency; The specific details of the testing environment are described in Section \ref{sec:exp_env}.)}
\label{fig:simplecmp}
\end{figure}


To evaluate these patterns, we implemented three representative strategies and analyzed their performance (Figure \ref{fig:simplecmp}). For the Global Scheduler Only pattern, we tested two approaches: the \textbf{Hash-Based Strategy}, which maintains a single instance per function to minimize cold starts but suffers from high execution latency under load, and the \textbf{No-Scaler Greedy Strategy}, which creates multiple replicas but leads to over-provisioning. For the Decoupled pattern, we implemented the \textbf{HPA Scaler + Greedy Strategy}, which shows detection lag in scaling decisions and increased cold starts due to poor coordination. As shown in the experimental results, neither pattern achieves satisfactory performance, motivating the need for better coordination between scaling and scheduling decisions.


\subsection{Existing Improvements}

\textbf{Advanced Scaling:} Systems like Hansel \cite{lstm_hansel} and Autopilot \cite{autopilot} have introduced predictive and learning-based scaling mechanisms to better handle dynamic workloads. However, these solutions still operate independently from scheduling decisions.

\textbf{Improved Scheduling:} Platforms like FaaSFlow and FnSched \cite{fnsched} have developed DAG-aware and hybrid scheduling strategies. Yet, they typically lack coordination with scaling components.

While these improvements have enhanced individual components, they fail to address the fundamental issue: the lack of coordination between scaling and scheduling decisions. This limitation motivates our investigation into a joint optimization approach that can bridge this gap.

