<?php

namespace App\Observers;

use Exception;
use App\Inventory;
use App\Handlers\ProducerHandler;
use Illuminate\Support\Facades\Log;

class InventoryObserver
{
    /**
     * Topic name
     */
    const KAFKA_TOPIC = 'inventories';

    /**
     * Publish error message
     */
    const PUBLISH_ERROR_MESSAGE = 'Publish message to kafka failed';

    /**
     * Kafka producer
     *
     * @var ProducerHandler
     */
    protected $producerHandler;

    /**
     * InventoryObserver's constructor
     *
     * @param ProducerHandler $producerHandler producerHandler
     */
    public function __construct(ProducerHandler $producerHandler)
    {
        $this->producerHandler = $producerHandler;
    }

    /**
     * Push inventory to kafka
     *
     * @param Inventory $inventory inventory
     *
     * @return void
     */
    protected function pushToKafka(Inventory $inventory)
    {
        try {
            $this->producerHandler
                ->setTopic(self::KAFKA_TOPIC)
                ->send($inventory->toJSON(), $inventory->id);
        } catch (Exception $e) {
            Log::critical(self::PUBLISH_ERROR_MESSAGE, [
                'code' => $e->getCode(),
                'error' => $e->getMessage(),
            ]);
        }
    }

    /**
     * Handle the inventory "created" event.
     *
     * @param  Inventory $inventory inventory
     *
     * @return void
     */
    public function created(Inventory $inventory)
    {
        $this->pushToKafka($inventory);
    }

    /**
     * Handle the inventory "updated" event.
     *
     * @param  Inventory $inventory inventory
     *
     * @return void
     */
    public function updated(Inventory $inventory)
    {
        $this->pushToKafka($inventory);
    }

    /**
     * Handle the inventory "deleted" event.
     *
     * @param  Inventory $inventory inventory
     *
     * @return void
     */
    public function deleted(Inventory $inventory)
    {
        $this->pushToKafka($inventory);
    }
}
