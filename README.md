# COMPSs-Mobile

##Table of contents
1. [Project motivation](#motivation)
2. [Project description](#description)
3. [Branch notes](#branch)
4. [Team Members](#members)
5. [License](#license)





## <a name="motivation"></a>Project Motivation
In the recent years, we have assisted to a revolution on IT technologies. The traditional centralized paradigm, where the whole application is hosted in local resources, evolves into a distributed model where users have a simple device to interact with the application but the heavy-weighted computation is performed remotely.

On the one hand, the popularization of mobile devices, such as smart-phones and tablets, have changed the way people access IT services. People permanently bear a computing device that provides them with immediate access to computing services that support them on their work or daily life. For instance, a doctor who is visiting interned patients in their rooms queries their evolution within the last hours to decide a treatment.

On the other hand, the Cloud has emerged as the response to the growing need of computing power. Cloud technologies allow any person or organization to use an infinity of computing resource. These services have reduced the costs of having a large computing infrastructure by transforming the expenses of purchase, maintenance and operation into a pay-as-you-go bill.

Mobile Cloud Computing (MCC) brings together the benefits of both: it gathers the immediacy of access of mobile devices with the infinite computing capacity of the Cloud. Thus, mobile users can increase the computing capacity of their devices and solve more complex computational problems. Instead of consulting the evolution of the patients, the doctors could simulate the impact of many treatments on them and pick the most suitable one.

Developing applications that fully exploit MCC is not straight-forward. To achieve a high performance on complex applications, developers must face all the issues of parallelizing the application and the distribution of its components. In addition, the developer has to deal with the rapid variability of the network conditions induced by the high mobility of the mobile device. Applications should adapt their execution according to the current conditions; thus avoids harming the energy-efficiency and performance of the application. Facing these issues taking into account all the variables requires a high level of expertise. For experts in distributed computing, dealing with them means to increase the development time of the application. For developers without the expertise, it means an impassable wall.




## <a name="description"></a>Project Description
COMP Superscalar (COMPSs) is a programming framework that provides a programming model for developing sequential applications and a runtime toolkit that executes the applications described following this model in parallel on top of distributed infrastructures. The execution model is based on how superscalar microprocessors execute instructions in an out-of-order manner. Instructions to be executed in the processor have input and output registers, and to guarantee that an out-of-order execution will produce a correct result, data dependencies between instructions need to be detected. In this analogy with superscalar microprocessors applied to a software level, instructions correspond to method calls inside an application, and the registers correspond to the data that a method is reading or writing. The runtime toolkit that supports the model automatically creates a work-flow that describes data dependencies between method calls. Besides, as it is done in superscalar processors, data renaming techniques can be 
applied in order to eliminate false data dependencies (WaR, WaW). In summary, with this idea we enable sequential applications to run in parallel in a distributed infrastructure.

## <a name="branch"></a>Branch Notes
This is the master branch of COMPSs-Mobile targeted to Java-compatible devices.

## <a name="members"></a>Team Members
Francesc Lordan

Rosa Mª Badia

## <a name="license"></a>License
Copyright 2008-2016 Barcelona Supercomputing Center (http://www.bsc.es)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 
