/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Adaptive.Agrona
{
	/// <summary>
	/// Implementations of this interface can a resource that need to have external state tracked for deletion.
	/// </summary>
	public interface IManagedResource
	{
		/// <summary>
		/// Set the time of the last state change.
		/// </summary>
		/// <param name="time"> of the last state change. </param>
		void TimeOfLastStateChange(long time);

		/// <summary>
		/// Get the time of the last state change.
		/// </summary>
		/// <returns> the time of the last state change. </returns>
		long TimeOfLastStateChange();

		/// <summary>
		/// Delete any resources held. This method should be idempotent.
		/// </summary>
		void Delete();
	}
}