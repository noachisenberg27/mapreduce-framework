

//-------------------------------------- Includes --------------------------------------------------


#include <algorithm>
#include <iostream>
#include <map>
#include <semaphore.h>
#include <queue>
#include "MapReduceFramework.h"
#include <fstream>
#include <sys/time.h>
#include <ctime>


//-------------------------------- system call macros ----------------------------------------------


#define LOCK_CONTAINER_MUTEX if (pthread_mutex_lock(&(emit2[pthread_self()].first)))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_lock failed."<<std::endl;\
        exit(1);\
    }


#define UNLOCK_CONTAINER_MUTEX if (pthread_mutex_unlock(&(emit2[pthread_self()].first)))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_unlock failed."<<std::endl;\
        exit(1);\
    }

#define SEM_POST_SHUFFLER if (sem_post(&shuffler))\
    {\
        std::cerr<<"MapReduceFramework Failure: sem_post failed."<<std::endl;\
        exit(1);\
    }

#define LOCK_LOG_FILE_MUTEX if (pthread_mutex_lock(&log_file_mutex))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_lock failed."<<std::endl;\
        exit(1);\
    }


#define UNLOCK_LOG_FILE_MUTEX if (pthread_mutex_unlock(&log_file_mutex))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_unlock failed."<<std::endl;\
        exit(1);\
    }
#define SEM_WAIT_SHUFFLER if (sem_wait(&shuffler))\
        {\
            std::cerr<<"MapReduceFramework Failure: sem_wait failed."<<std::endl;\
            exit(1);\
        }
#define LOCK_AMOUNT_OF_UNFINISHED_MAP_THREADS_MUTEX if (pthread_mutex_lock(&amount_of_unfinished_map_threads_mutex))\
        {\
            std::cerr<<"MapReduceFramework Failure: pthread_mutex_lock failed."<<std::endl;\
            exit(1);\
        }
#define UNLOCK_AMOUNT_OF_UNFINISHED_MAP_THREADS_MUTEX if (pthread_mutex_unlock(&amount_of_unfinished_map_threads_mutex))\
        {\
            std::cerr<<"MapReduceFramework Failure: pthread_mutex_unlock failed."<<std::endl;\
            exit(1);\
        }
#define LOCK_CONTAINER_MUTEX_BY_SHUFFLER if (pthread_mutex_lock(&(container.second.first)))\
                {\
                    std::cerr<<"MapReduceFramework Failure: pthread_mutex_lock failed."<<std::endl;\
                    exit(1);\
                }
#define UNLOCK_CONTAINER_MUTEX_BY_SHUFFLER if (pthread_mutex_unlock(&(container.second.first)))\
                {\
                    std::cerr<<"MapReduceFramework Failure: pthread_mutex_unlock failed."<<std::endl;\
                    exit(1);\
                }
#define LOCK_PTHREAD_TO_CONTAINER_MUTEX if (pthread_mutex_lock(&pthreadToContainer_mutex))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_lock failed."<<std::endl;\
        exit(1);\
    }
#define UNLOCK_PTHREAD_TO_CONTAINER_MUTEX if (pthread_mutex_unlock(&pthreadToContainer_mutex))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_unlock failed."<<std::endl;\
        exit(1);\
    }
#define LOCK_MAP_POS_MUTEX if (pthread_mutex_lock(&map_pos_mutex))\
        {\
            std::cerr<<"MapReduceFramework Failure: pthread_mutex_lock failed."<<std::endl;\
            exit(1);\
        }
#define UNLOCK_MAP_POS_MUTEX if (pthread_mutex_unlock(&map_pos_mutex))\
            {\
                std::cerr<<"MapReduceFramework Failure: pthread_mutex_unlock failed."<<std::endl;\
                exit(1);\
            }
#define LOCK_REDUCE_POS_MUTEX if(pthread_mutex_lock(&reduce_pos_mutex))\
        {\
            std::cerr<<"MapReduceFramework Failure: pthread_mutex_lock failed."<<std::endl;\
            exit(1);\
        }
#define UNLOCK_REDUCE_POS_MUTEX if (pthread_mutex_unlock(&reduce_pos_mutex))\
            {\
                std::cerr<<"MapReduceFramework Failure: pthread_mutex_unlock failed."<<std::endl;\
                exit(1);\
            }
#define GET_TIME_OF_DAY_FOR_BEGINNING if (gettimeofday(&beginningTime,NULL))\
    {\
        std::cerr<<"MapReduceFramework Failure: gettimeofday failed."<<std::endl;\
        exit(1);\
    }
#define LOG_FILE_MUTEX_INIT if (pthread_mutex_init(&log_file_mutex, NULL))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_init failed."<<std::endl;\
        exit(1);\
    }
#define PTHREAD_TO_CONTAINER_MUTEX_INIT if (pthread_mutex_init(&pthreadToContainer_mutex, NULL))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_init failed."<<std::endl;\
        exit(1);\
    }
#define MAP_POS_MUTEX_INIT if (pthread_mutex_init(&map_pos_mutex, NULL))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_init failed."<<std::endl;\
        exit(1);\
    }
#define REDUCE_POS_MUTEX_INIT if (pthread_mutex_init(&reduce_pos_mutex, NULL))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_init failed."<<std::endl;\
        exit(1);\
    }
#define AMOUNT_OF_UN_FINISHED_MAP_THREADS_MUTEX_INIT if (pthread_mutex_init(&amount_of_unfinished_map_threads_mutex, NULL))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_init failed."<<std::endl;\
        exit(1);\
    }
#define SHUFFLER_SEM_INIT if (sem_init(&shuffler, 0, 0))\
    {\
        std::cerr<<"MapReduceFramework Failure: sem_init failed."<<std::endl;\
        exit(1);\
    }
#define CONTAINER_MUTEX_INIT if (pthread_mutex_init(&mutexs[k], NULL))\
            {\
                std::cerr<<"MapReduceFramework Failure: pthread_mutex_init failed."<<std::endl;\
                exit(1);\
            }
#define PTHREAD_CREATE_SHUFFLER if (pthread_create(&thread_pool[multiThreadLevel], NULL, shufflefunc, &autoDeleteV2K2))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_create failed."<<std::endl;\
        exit(1);\
    }
#define PTHREAD_JOIN_ON_MAP if (pthread_join(thread_pool[i], NULL))\
        {\
            std::cerr<<"MapReduceFramework Failure: pthread_join failed."<<std::endl;\
            exit(1);\
        }
#define GET_TIME_OF_DAY_FOR_ENDING if (gettimeofday(&endingTime,NULL))\
    {\
        std::cerr<<"MapReduceFramework Failure: gettimeofday failed."<<std::endl;\
        exit(1);\
    }
#define PTHREAD_CREATE_REDUCE if (pthread_create(&thread_pool[k], NULL, runReduce, (void *) &mapReduceData))\
        {\
            std::cerr << "MapReduceFramework Failure: pthread_create failed." << std::endl;\
            exit(1);\
        }
#define PTHREAD_JOIN_ON_REDUCE if (pthread_join(thread_pool[j], NULL))\
        {\
            std::cerr<<"MapReduceFramework Failure: pthread_join failed."<<std::endl;\
            exit(1);\
        }
#define PTHREAD_TO_CONTAINER_MUTEX_DESTROY if (pthread_mutex_destroy(&pthreadToContainer_mutex))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_destroy failed."<<std::endl;\
        exit(1);\
    }
#define MAP_POS_MUTEX_DESTROY if (pthread_mutex_destroy(&map_pos_mutex))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_destroy failed."<<std::endl;\
        exit(1);\
    }
#define REDUCE_POS_MUTEX_DESTROY if (pthread_mutex_destroy(&reduce_pos_mutex))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_destroy failed."<<std::endl;\
        exit(1);\
    }
#define AMOUNT_OF_UNFINSHED_MAP_THREADS_MUTEX_DESTROY if (pthread_mutex_destroy(&amount_of_unfinished_map_threads_mutex))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_destroy failed."<<std::endl;\
        exit(1);\
    }
#define LOG_FILE_MUTEX_DESTROY if (pthread_mutex_destroy(&log_file_mutex))\
    {\
        std::cerr<<"MapReduceFramework Failure: pthread_mutex_destroy failed."<<std::endl;\
        exit(1);\
    }
#define SHUFFLER_SEM_DESTROY if (sem_destroy(&shuffler))\
    {\
        std::cerr<<"MapReduceFramework Failure: sem_destroy failed."<<std::endl;\
        exit(1);\
    }
#define GET_TIME_FORAMT if (!(std::strftime(buffer, 80, "[%d.%m.%Y %H:%M:%S]", timeinfo)))\
    {\
        std::cerr<<"MapReduceFramework Failure: std::strftime failed."<<std::endl;\
        exit(1);\
    }
#define STD_TIME_INIT if (std::time(&rawtime) == (std::time_t)(-1))\
    {\
        std::cerr<<"MapReduceFramework Failure: std::time failed."<<std::endl;\
        exit(1);\
    }
#define STD_LOCALTIME_CHECK if (!rawtime){\
        std::cerr<<"MapReduceFramework Failure: std::localtime failed."<<std::endl;\
        exit(1);\
    }
#define OPEN_FILE_CHECK if (! logfile.is_open())\
    {\
        std::cerr<<"MapReduceFramework Failure: open file failed."<<std::endl;\
        exit(1);\
    }
#define CLOSE_FILE_CHECK if (logfile.is_open())\
    {\
        std::cerr<<"MapReduceFramework Failure: close time failed."<<std::endl;\
        exit(1);\
    }


//-------------------------------------  typedefs --------------------------------------------------


typedef std::pair<k2Base *, v2Base *> k2v2Pair;

typedef std::pair<pthread_mutex_t, std::queue<k2v2Pair>> mutexAndQueue;

typedef std::vector<std::pair<k3Base *, v3Base *>> k3v3Vector;



//------------------------------------  Helper class -----------------------------------------------

/**
 * @brief This class is the way to send the MapReduceBase and IN_ITEMS_VEC elements to all threads.
 */
class MapReduceData
{
public:
    MapReduceData(MapReduceBase &mapReduce1, IN_ITEMS_VEC &itemsVec1)
    {
        mapReduce = &mapReduce1;
        itemsVec = &itemsVec1;
    }

    MapReduceBase *mapReduce;
    IN_ITEMS_VEC *itemsVec;
};


//---------------------------------  shuffle Comparator --------------------------------------------


/**
 * @brief This is the comparator to compare between K2Base pointers needed for the container the
 *        shuffle creates.
 */
struct compareK2ByValue
{
    bool operator()(k2Base *const &k2Base1, k2Base *const &k2Base2) const
    {
        return (*k2Base1) < (*k2Base2);
    }
};

//----------------------------------  emit3 Comparator ---------------------------------------------

/**
 * @brief The comparator to use sorting the result vector.
 * @param firstElem The first element to compare.
 * @param secondElem The second element to compare.
 * @return True if the first element is smaller from the second element.
 */
bool pairCompare(const std::pair<k3Base *, v3Base *> &firstElem,
                 const std::pair<k3Base *, v3Base *> &secondElem)
{
    return *(firstElem.first) < *(secondElem.first);

}


//------------------------------------  Containers -------------------------------------------------

/**
 * @brief This is the map that will contain the k2Base and v2Base elements the user uses Emit2 to
 *        write in his map function.
 *        The mutex is so the user wont write to the container while the shuffler is reading from
 *        it.
 */
std::map<pthread_t, mutexAndQueue> emit2;

/**
 * @brief This is the map that will contain the result of the shuffles work, meaning all the
 *        k2Bases the suffler collected and all v2Base objects matching a given k2Base.
 */
std::map<k2Base *, std::vector<v2Base *>, compareK2ByValue> shuffle;

/**
 * @brief This is a container so the reduce can run over chunks of data from the map in some order.
 *        Since a map has no order there is no logicial way to iterate over the data so this
 *        vectors coordinates will be the logic under the iteration over the shufflers map.
 */
std::vector<k2Base *> shuffle_it;

/**
 * @brief This is the map that will contain the k3Base and v3Base elements the user uses Emit3 to
 *        write in his reduce function.
 */
std::map<pthread_t, k3v3Vector> emit3;


//------------------------------  Mutex and Semaphore elements  ------------------------------------


/**
 * @brief This is the mutex blocking all threads so the emit2 container can be created before
 *        being accessed.
 */
pthread_mutex_t pthreadToContainer_mutex;

/**
 * @brief This is the mutex gaurding the position of the next chunk of map data.
 */
pthread_mutex_t map_pos_mutex;

/**
 * @brief This is the position of the next chunk of map data.
 */
int map_pos = 0;

/**
 * @brief This is the mutex gaurding the position of the next chunk of reduce data.
 */
pthread_mutex_t reduce_pos_mutex;

/**
 * @brief This is the position of the next chunk of reduce data.
 */
int reduce_pos = 0;

/**
 * @brief This is the mutex gaurding the amount of threads that are not finished with their map
 *        stage.
 */
pthread_mutex_t amount_of_unfinished_map_threads_mutex;

/**
 * @brief This is the mutex gaurding the amount of threads that are not finished with their map
 *        stage.
 */
pthread_mutex_t log_file_mutex;

/**
 * @brief This is the amount of threads that didnt finish their map stage.
 */
int amount_of_unfinished_map_threads;

/**
 * @brief This is the semaphore that notifies the shuffler if there is data to sort during the map
 *        stage.
 */
sem_t shuffler;


//------------------------------------  Global elements --------------------------------------------

/**
 * @brief This is the log file element we will create.
 */
std::ofstream logfile;

/**
 * @brief The timeofday elements.
 */
timeval beginningTime, endingTime;

/**
 * @brief The elements for the time.
 */
std::time_t rawtime;
std::tm *timeinfo;
char buffer[80];


//----------------------------------  Framework functions ------------------------------------------


/**
 * @brief The emit2 function the map function can use to store results.
 * @param k2 The key to store.
 * @param v2 The value to store.
 */
void Emit2(k2Base *k2, v2Base *v2)
{
    LOCK_CONTAINER_MUTEX;
    emit2[pthread_self()].second.push(std::pair<k2Base *, v2Base *>(k2, v2));
    UNLOCK_CONTAINER_MUTEX;
    SEM_POST_SHUFFLER;
}


/**
 * @brief The emit3 function the reduce function can use to store results.
 * @param k3 The key to store.
 * @param v3 The value to store.
 */
void Emit3(k3Base *k3, v3Base *v3)
{
    emit3[pthread_self()].push_back(std::pair<k3Base *, v3Base *>(k3, v3));
}


/**
 * @brief The function the shuffling thread should run for preparing the data for the reduce
 *        function.
 * @param delete_k2v2 A flag representing if the framework should delete the k2Base and v2Base
 *        elements.
 * @return The thread terminating itself.
 */
void *shufflefunc(void *delete_k2v2)
{
    LOCK_LOG_FILE_MUTEX;
    GET_TIME_FORAMT;
    logfile << "Thread Shuffle created " << buffer << "\n";
    UNLOCK_LOG_FILE_MUTEX;
    bool to_delete_k2v2 = *(bool *) delete_k2v2;
    bool finished_shuffling = false;
    while (true)
    {
        SEM_WAIT_SHUFFLER;
        LOCK_AMOUNT_OF_UNFINISHED_MAP_THREADS_MUTEX;
        if (amount_of_unfinished_map_threads == 0)
        {
            finished_shuffling = true;
        }
        UNLOCK_AMOUNT_OF_UNFINISHED_MAP_THREADS_MUTEX;
        for (auto &container: emit2)
        {
            if (!container.second.second.empty())
            {
                LOCK_CONTAINER_MUTEX_BY_SHUFFLER;
                while (true)
                {
                    std::pair<k2Base *, v2Base *> element = container.second.second.front();
                    container.second.second.pop();
                    if (shuffle.find(element.first) == shuffle.end())
                    {
                        //NOT found.
                        shuffle_it.push_back(element.first);
                        std::vector<v2Base *> vector;
                        vector.push_back(element.second);
                        shuffle.insert(
                                std::pair<k2Base *, std::vector<v2Base *>>(element.first, vector));
                    }
                    else
                    {
                        //Found.
                        shuffle[element.first].push_back(element.second);
                        if (to_delete_k2v2)
                        {
                            delete element.first;
                        }
                    }
                    if (container.second.second.empty())
                    {
                        break;
                    }
                    SEM_WAIT_SHUFFLER;
                }
                UNLOCK_CONTAINER_MUTEX_BY_SHUFFLER;
            }
        }
        if (finished_shuffling)
        {
            break;
        }
    }
    LOCK_LOG_FILE_MUTEX;
    GET_TIME_FORAMT;
    logfile << "Thread Shuffle terminated " << buffer << "\n";
    UNLOCK_LOG_FILE_MUTEX;
    pthread_exit(NULL);
}


/**
 * @brief The function the thread running the map should run.
 * @param mapReduceData A helper class containing the map and reduce functions and a thread
 *        containing the k1Base and v1Base elements.
 * @return The thread terminating itself.
 */
void *runMap(void *mapReduceData)
{
    LOCK_LOG_FILE_MUTEX;
    GET_TIME_FORAMT;
    logfile << "Thread ExecMap created " << buffer << "\n";
    UNLOCK_LOG_FILE_MUTEX;
    LOCK_PTHREAD_TO_CONTAINER_MUTEX;
    UNLOCK_PTHREAD_TO_CONTAINER_MUTEX;
    MapReduceData mapReduce = *((MapReduceData *) mapReduceData);
    int pos, chunkSize;
    while (true)
    {
        LOCK_MAP_POS_MUTEX;
        if ((unsigned int)map_pos == mapReduce.itemsVec->size())
        {
            UNLOCK_MAP_POS_MUTEX;
            break;
        }
        else
        {
            pos = map_pos;
            chunkSize = std::min(10, (int) (mapReduce.itemsVec->size() - map_pos));
            map_pos += chunkSize;
            UNLOCK_MAP_POS_MUTEX;
        }
        for (int i = 0; i < chunkSize; ++i)
        {
            mapReduce.mapReduce->Map(((*mapReduce.itemsVec)[pos + i]).first,
                                     ((*mapReduce.itemsVec)[pos + i]).second);
        }
    }
    LOCK_AMOUNT_OF_UNFINISHED_MAP_THREADS_MUTEX;
    amount_of_unfinished_map_threads -= 1;
    UNLOCK_AMOUNT_OF_UNFINISHED_MAP_THREADS_MUTEX;
    SEM_POST_SHUFFLER;
    LOCK_LOG_FILE_MUTEX;
    GET_TIME_FORAMT;
    logfile << "Thread ExecMap terminated " << buffer << "\n";
    UNLOCK_LOG_FILE_MUTEX;
    pthread_exit(NULL);
}


/**
 * @brief The function the thread running reduce should run.
 * @param mapReduceData A helper class containing the map and reduce functions and a thread
 *        containing the k1Base and v1Base elements.
 * @return The thread terminating itself.
 */
void *runReduce(void *mapReduceData)
{
    LOCK_LOG_FILE_MUTEX;
    GET_TIME_FORAMT;
    logfile << "Thread ExecReduce created " << buffer << "\n";
    UNLOCK_LOG_FILE_MUTEX;
    MapReduceData mapReduce = *((MapReduceData *) mapReduceData);
    int pos, chunkSize;
    while (true)
    {
        LOCK_REDUCE_POS_MUTEX;
        if ((unsigned int)reduce_pos == shuffle_it.size())
        {
            UNLOCK_REDUCE_POS_MUTEX;
            break;
        }
        else
        {
            pos = reduce_pos;
            chunkSize = std::min(10, (int) shuffle_it.size() - reduce_pos);
            reduce_pos += chunkSize;
            UNLOCK_REDUCE_POS_MUTEX;
        }
        for (int j = 0; j < chunkSize; ++j)
        {
            mapReduce.mapReduce->Reduce(shuffle_it[pos + j], shuffle[shuffle_it[pos + j]]);
        }
    }
    LOCK_LOG_FILE_MUTEX;
    GET_TIME_FORAMT;
    logfile << "Thread ExecReduce terminated " << buffer << "\n";
    UNLOCK_LOG_FILE_MUTEX;
    pthread_exit(NULL);
}

/**
 * @brief The main thread managing the creation of other threads and creation of containers the
 *        other threads will use.
 * @param mapReduce A class containing the map and reduce functions to run.
 * @param itemsVec The vector containing the k1Base and v1Base elements the map functions should
 *        get as parameters.
 * @param multiThreadLevel The amount of threads to create to run the map and reduce functions.
 * @param autoDeleteV2K2 A flag representing if the framework should delete the k2Base and v2Base
 *        elements that wheir allocated on the heap.
 * @return The result vector with the result of the reduce function after being sorted.
 */
OUT_ITEMS_VEC
RunMapReduceFramework(MapReduceBase &mapReduce, IN_ITEMS_VEC &itemsVec, int multiThreadLevel,
                      bool autoDeleteV2K2)
{
    STD_TIME_INIT;
    timeinfo = std::localtime(&rawtime);
    STD_LOCALTIME_CHECK;
    GET_TIME_OF_DAY_FOR_BEGINNING;
    LOG_FILE_MUTEX_INIT;
    logfile.open(".MapReduceFramework.log", std::ios::app);
    OPEN_FILE_CHECK;
    LOCK_LOG_FILE_MUTEX;
    logfile << "RunMapReduceFramework started with " << multiThreadLevel << " threads\n";
    UNLOCK_LOG_FILE_MUTEX;
    PTHREAD_TO_CONTAINER_MUTEX_INIT;
    MAP_POS_MUTEX_INIT;
    REDUCE_POS_MUTEX_INIT;
    AMOUNT_OF_UN_FINISHED_MAP_THREADS_MUTEX_INIT;
    amount_of_unfinished_map_threads = multiThreadLevel;
    MapReduceData mapReduceData(mapReduce, itemsVec);
    std::vector<pthread_t> thread_pool((unsigned long) multiThreadLevel + 1);
    pthread_mutex_t mutexs[multiThreadLevel];
    LOCK_PTHREAD_TO_CONTAINER_MUTEX;
    SHUFFLER_SEM_INIT;
    emit2.clear();
    for (auto &container: emit3)
    {
        container.second.clear();
    }
    emit3.clear();
    for (auto &container: shuffle)
    {
        container.second.clear();
    }
    shuffle.clear();
    shuffle_it.clear();
    map_pos = 0;
    reduce_pos = 0;
    for (int k = 0; k < multiThreadLevel; ++k)
    {
        if (pthread_create(&thread_pool[k], NULL, runMap, (void *) &mapReduceData))
        {
            std::cerr << "MapReduceFramework Failure: pthread_create failed" << std::endl;
            exit(1);
        }
        else
        {
            std::queue<std::pair<k2Base *, v2Base *>> queue;
            mutexAndQueue container;
            CONTAINER_MUTEX_INIT;
            container = std::make_pair(mutexs[k], queue);
            emit2.insert(std::pair<pthread_t, mutexAndQueue>(thread_pool[k], container));
            std::vector<std::pair<k3Base *, v3Base *>> vector;
            emit3.insert(std::pair<pthread_t, k3v3Vector>(thread_pool[k], vector));
        }
    }
    UNLOCK_PTHREAD_TO_CONTAINER_MUTEX;
    PTHREAD_CREATE_SHUFFLER;
    for (int i = 0; i < (int)thread_pool.size(); ++i)
    {
        PTHREAD_JOIN_ON_MAP;
    }
    GET_TIME_OF_DAY_FOR_ENDING;
    LOCK_LOG_FILE_MUTEX;
    logfile << "Map and Shuffle took " << (endingTime.tv_sec - beginningTime.tv_sec) * 1000000000 +
                                          (endingTime.tv_usec - beginningTime.tv_usec) * 1000
            << " ns\n";
    UNLOCK_LOG_FILE_MUTEX;
    GET_TIME_OF_DAY_FOR_BEGINNING;
    for (int k = 0; k < multiThreadLevel; ++k)
    {
        PTHREAD_CREATE_REDUCE;
    }
    for (int j = 0; j < (int)thread_pool.size() - 1; ++j)
    {
        PTHREAD_JOIN_ON_REDUCE;
    }
    std::vector<std::pair<k3Base *, v3Base *>> temp;
    for (auto const &x: emit3)
    {
        temp.insert(temp.end(), x.second.begin(), x.second.end());
    }
    if (autoDeleteV2K2)
    {
        for (auto &container: shuffle)
        {
            delete container.first;
            for (auto &v2: container.second)
            {
                delete v2;
            }
        }
    }
    std::sort(temp.begin(), temp.end(), pairCompare);
    GET_TIME_OF_DAY_FOR_ENDING;
    logfile << "Reduce took " << (endingTime.tv_sec - beginningTime.tv_sec) * 1000000000 +
                                 (endingTime.tv_usec - beginningTime.tv_usec) * 1000 << " ns\n";
    PTHREAD_TO_CONTAINER_MUTEX_DESTROY;
    MAP_POS_MUTEX_DESTROY;
    REDUCE_POS_MUTEX_DESTROY;
    AMOUNT_OF_UNFINSHED_MAP_THREADS_MUTEX_DESTROY;
    LOG_FILE_MUTEX_DESTROY;
    SHUFFLER_SEM_DESTROY;
    logfile << "RunMapReduceFramework finished\n";
    logfile.close();
    CLOSE_FILE_CHECK;
    return temp;
}