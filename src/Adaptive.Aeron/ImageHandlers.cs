/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Adaptive.Aeron
{
    /// <summary>
    /// Interface for notification of new <see cref="Image"/>s becoming available under a <see cref="Subscription"/>
    /// 
    /// Method called by Aeron to deliver notification of a new <see cref="Image"/> being available for polling.
    /// <param name="image"> that is now available.</param>
    /// </summary>
    public delegate void AvailableImageHandler(Image image);

    /// <summary>
    /// Interface for delivery of inactive image notification to a <seealso cref="Subscription"/>.
    /// 
    /// Method called by Aeron to deliver notification that an <see cref="Image"/> is no longer available for polling.
    /// </summary>
    /// <param name="image"> the image that has become unavailable.</param>
    public delegate void UnavailableImageHandler(Image image);
}