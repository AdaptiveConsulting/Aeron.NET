﻿/*
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
    /// 
    /// Within this callback reentrant calls to the <see cref="Aeron"/> client are not permitted and
    /// will result in undefined behaviour.
    /// </summary>
    /// <param name="image"> that is now available.</param>
    public delegate void AvailableImageHandler(Image image);
}