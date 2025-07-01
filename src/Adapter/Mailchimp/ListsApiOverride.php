<?php

/**
 * @package Telegraph
 * @license http://opensource.org/licenses/MIT
 */

declare(strict_types=1);

namespace DecodeLabs\Telegraph\Adapter\Mailchimp;

use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Uri;
use MailchimpMarketing\Api\ListsApi;

class ListsApiOverride extends ListsApi
{
    /**
     * This override method is used to set the count query parameter to 1000
     * otherwise the API offers no way to request more than 10
     */
    protected function tagSearchRequest(
        mixed $list_id,
        mixed $name = null
    ): Request {
        $request = parent::tagSearchRequest($list_id, $name);
        $url = Uri::withQueryValue($request->getUri(), 'count', '1000');
        return $request->withUri($url);
    }
}
